package org.apache.minibase.storage;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CommitLog {
    private final String fileDir;
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final LinkedBlockingQueue<KeyValue> kvBuffer = new LinkedBlockingQueue<>(200_000);

    public static final String WAL_PREFIX = "minibase_commit_log";

    public CommitLog(String fileDir) {
        this.fileDir = fileDir;
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);
        scheduledExecutorService.scheduleAtFixedRate(()->{
            ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();
            writeLock.lock();
            try {
                FileOutputStream fileOutputStream = new FileOutputStream(fileDir + File.separator + WAL_PREFIX, true);
                List<KeyValue> kvList = new ArrayList<>(kvBuffer.size() + 1);
                kvBuffer.drainTo(kvList);
                for (KeyValue kv : kvList) {
                    byte[] bytes = serialize(kv);
                    fileOutputStream.write(bytes);
                }
            } catch (IOException e) {

            } finally {
                writeLock.unlock();
            }
        }, 1000, 5000, TimeUnit.MILLISECONDS);
    }

    private byte[] serialize(KeyValue kv) {
        return new byte[0];
    }

    public boolean write(KeyValue kv) {
        kvBuffer.put(kv);
        return true;
    }

    public void nextFile() {

    }

    public void deleteFormerFile() {

    }
}
