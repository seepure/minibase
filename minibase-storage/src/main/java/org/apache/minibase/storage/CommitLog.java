package org.apache.minibase.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CommitLog {
    private static final Logger LOG = Logger.getLogger(CommitLog.class);
    private final String fileDir;
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final LinkedBlockingQueue<KeyValue> kvBuffer = new LinkedBlockingQueue<>(30_000);
    private final ScheduledExecutorService scheduledExecutorService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public static final String CURRENT_FILE = "minibase_commit_log.current";
    public static final String PRE_FILE = "minibase_commit_log.pre";

    public CommitLog(String fileDir, ReadKvCallBack callBack) throws IOException {
        this.fileDir = fileDir;
        replay(callBack);
        scheduledExecutorService = Executors.newScheduledThreadPool(2);
        scheduledExecutorService.scheduleAtFixedRate(()->{
            ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
            writeLock.lock();
            FileOutputStream fileOutputStream = null;
            PrintWriter pw = null;
            try {
                fileOutputStream = new FileOutputStream(fileDir + File.separator + CURRENT_FILE, true);
                pw = new PrintWriter(fileOutputStream);
                List<KeyValue> kvList = new ArrayList<>(kvBuffer.size() + 1);
                kvBuffer.drainTo(kvList);
                for (KeyValue kv : kvList) {
//                    byte[] bytes = serialize(kv);
//                    fileOutputStream.write(bytes);
                    String s = serializeAsString(kv);
                    pw.println(s);
                }
            } catch (IOException e) {

            } finally {
                if (pw != null)
                    pw.close();
                if (fileOutputStream != null) {
                    try {
                        fileOutputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                writeLock.unlock();
            }
        }, 500, 500, TimeUnit.MILLISECONDS);
    }

    private byte[] serialize(KeyValue kv) {
        return new byte[0];
    }

    private String serializeAsString(KeyValue kv) throws IOException {
        return objectMapper.writeValueAsString(kv);
    }

    private KeyValue deserialize(byte[] bytes) {
        return null;
    }

    private KeyValue deserialize(String line) throws IOException {
        return objectMapper.readValue(line, KeyValue.class);
    }

    public boolean write(KeyValue kv) {
        try {
            kvBuffer.put(kv);
        } catch (InterruptedException e) {
            return false;
        }
        return true;
    }

    public void nextFile() {

    }

    public void deleteFormerFile() {

    }

    protected void replay(ReadKvCallBack callBack) throws IOException {
        String preFilePath = fileDir + File.separator + PRE_FILE;
        String currentFilePath = fileDir + File.separator + CURRENT_FILE;
        if (new File(preFilePath).exists()) {
            replaySingleFile(preFilePath, callBack);
        }
        if (new File(currentFilePath).exists()) {
            replaySingleFile(currentFilePath, callBack);
        }

    }

    protected void replaySingleFile(String filePath, ReadKvCallBack callBack) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        try {
            String line = null;
            while ((line = br.readLine()) != null) {
                KeyValue keyValue = deserialize(line);
                callBack.replay(keyValue);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (br != null)
                br.close();
        }
    }

    interface ReadKvCallBack {
        void replay(KeyValue kv);
    }
}
