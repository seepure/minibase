package org.apache.minibase.server.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("minibase.storage")
//@PropertySource(value = "classpath:env_relevant.yml", encoding = "UTF-8", factory = YamlPropertyLoaderFactory.class)
public class MiniBaseConfig {

    Integer maxMemstoreSize;
    Integer flushMaxRetries;
    String dataDir;
    Integer maxDiskFiles;
    Integer maxThreadPoolSize;

    public Integer getMaxMemstoreSize() {
        return maxMemstoreSize;
    }

    public void setMaxMemstoreSize(Integer maxMemstoreSize) {
        this.maxMemstoreSize = maxMemstoreSize;
    }

    public Integer getFlushMaxRetries() {
        return flushMaxRetries;
    }

    public void setFlushMaxRetries(Integer flushMaxRetries) {
        this.flushMaxRetries = flushMaxRetries;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public Integer getMaxDiskFiles() {
        return maxDiskFiles;
    }

    public void setMaxDiskFiles(Integer maxDiskFiles) {
        this.maxDiskFiles = maxDiskFiles;
    }

    public Integer getMaxThreadPoolSize() {
        return maxThreadPoolSize;
    }

    public void setMaxThreadPoolSize(Integer maxThreadPoolSize) {
        this.maxThreadPoolSize = maxThreadPoolSize;
    }

}
