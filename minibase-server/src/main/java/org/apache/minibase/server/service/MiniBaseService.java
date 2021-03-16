package org.apache.minibase.server.service;

import org.apache.minibase.server.configuration.MiniBaseConfig;
import org.apache.minibase.storage.Config;
import org.apache.minibase.storage.KeyValue;
import org.apache.minibase.storage.MStore;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MiniBaseService implements InitializingBean, DisposableBean {

    private static final String CHARSET = "UTF8";
    private MStore mStore;
    @Autowired
    private MiniBaseConfig miniBaseConfig;

    @Override
    public void afterPropertiesSet() throws Exception {
        //todo 参数校验
        Config config = new Config();
        config.setMaxMemstoreSize(miniBaseConfig.getMaxMemstoreSize() * 1024 * 1024);
        config.setDataDir(miniBaseConfig.getDataDir());
        config.setFlushMaxRetries(miniBaseConfig.getFlushMaxRetries());
        config.setMaxDiskFiles(miniBaseConfig.getMaxDiskFiles());
        config.setMaxThreadPoolSize(miniBaseConfig.getMaxThreadPoolSize());

        mStore = MStore.create(config);
        mStore.open();
    }

    public void put(String key, String value) throws Exception {
        mStore.put(key.getBytes(CHARSET), value.getBytes(CHARSET));
    }

    public String get(String key) throws Exception {
        KeyValue keyValue = mStore.get(key.getBytes(CHARSET));
        return keyValue == null ? null : new String(keyValue.getValue(), CHARSET);
    }

    @Override
    public void destroy() throws Exception {
        if (mStore != null) {
            mStore.close();
        }
    }
}
