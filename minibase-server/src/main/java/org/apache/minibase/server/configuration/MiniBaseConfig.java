package org.apache.minibase.server.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties("minibase.storage")
//@PropertySource(value = "classpath:env_relevant.yml", encoding = "UTF-8", factory = YamlPropertyLoaderFactory.class)
public class MiniBaseConfig {
    Integer maxMemstoreSize;
}
