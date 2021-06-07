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
import java.util.concurrent.locks.ReentrantLock;

public class CommitLog {
    private static final Logger LOG = Logger.getLogger(CommitLog.class);
    private final String fileDir;
    private final ReentrantLock flushLock = new ReentrantLock();
    private final LinkedBlockingQueue<KeyValue> kvBuffer = new LinkedBlockingQueue<>(30_000);
    private final ScheduledExecutorService scheduledExecutorService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private volatile String currentFileName;
    public static final String FILE_CURRENT = "minibase_commit_log.current";
    public static final String FILE_PRE = "minibase_commit_log.pre";
    public static final String FILE_TO_BE_DEL = "minibase_commit_log.to_be_del";

    public CommitLog(String fileDir, ReadKvCallBack callBack) throws IOException {
        this.fileDir = fileDir;
        replay(callBack);
        if (currentFileName == null) {
            currentFileName = FILE_CURRENT;
        }
        scheduledExecutorService = Executors.newScheduledThreadPool(2);
        scheduledExecutorService.scheduleAtFixedRate(()->{
            flushLock.lock();
            try {
                flush();
            } finally {
                flushLock.unlock();
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
        //在这里主要担心的一个问题是在进行nextFile操作时FILE_PRE是否会已存在?
        //一般情况下不会存在！主要是因为在flushIfNeeded(true)中, 我们限制了在MemStore仍未完成snapshot flush到DiskStore时
        //kvMap大小达到getMaxMemstoreSize()的后续写入。也就意味着当前进行nextFile操作时，上一个FILE_PRE肯定已经删除了!
        //但是还是会出现存在上一个FILE_PRE的情况--
        // case 1. 比如snapshot已经完成了flush, 但是还没来得及删除FILE_PRE时发生了断电, 重启后还是会出现上一个FILE_PRE还存在的情况;
        // case 2. 上一次的snapshot的flush还没有完成, 但是发生了进程终止。
        // 在上述的情况下，只要服务重启了，那么肯定会有一个FILE_PRE
        // 为了应对上述的复杂情况，我们索性在replay commit-log的时候串行执行，如果commit-log当前在replay FILE_PRE时, 显然其对应的
        // 所有记录都应被flush到 DiskFile中! 所以在nextFile切换文件名时，应该什么都不做。
        if (FILE_PRE.equals(currentFileName)) {
            return;
        }

        //同时, 我们还要保证在切换文件名时，kvBuffer中的内容都被强制刷到了nextFile中, 这里会暂时将写中断! -- 这其实不好!
        if (kvBuffer.size() > 0) {
            flushLock.lock();
            try {
                flush();
            } finally {
                flushLock.unlock();
            }
        }

        if (FILE_CURRENT.equals(currentFileName)) {
            File file = new File(fileDir + File.separator + FILE_CURRENT);
            file.renameTo(new File(fileDir + File.separator + FILE_PRE));
        }
    }

    public void deleteStaledFile() {
        File filePre = new File(fileDir + File.separator + FILE_PRE);
        File fileToDel = new File(fileDir + File.separator + FILE_TO_BE_DEL);
        if (filePre.exists()) {
            filePre.renameTo(fileToDel);
        }
        if (fileToDel.exists()) {
            fileToDel.delete();
        }

    }

    protected void replay(ReadKvCallBack callBack) throws IOException {
        //我们需要保证在replay FILE_PRE
        String preFilePath = fileDir + File.separator + FILE_PRE;
        String currentFilePath = fileDir + File.separator + FILE_CURRENT;
        if (new File(preFilePath).exists()) {
            currentFileName = preFilePath;
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

    protected void flush() {
        if (kvBuffer.isEmpty()) {
            return;
        }
        FileOutputStream fileOutputStream = null;
        PrintWriter pw = null;
        try {
            fileOutputStream = new FileOutputStream(fileDir + File.separator + FILE_CURRENT, true);
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
            //todo 还需要将这个异常抛到上层
            LOG.error(e.getMessage(), e);
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
        }
    }

    interface ReadKvCallBack {
        void replay(KeyValue kv) throws IOException;
    }
}
