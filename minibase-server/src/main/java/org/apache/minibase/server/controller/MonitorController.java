package org.apache.minibase.server.controller;

import org.apache.minibase.server.configuration.MiniBaseConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "monitor")
public class MonitorController {
    @Autowired
    private MiniBaseConfig miniBaseConfig;

    @RequestMapping(value = "base_config", method = RequestMethod.GET)
    @ResponseBody
    public MiniBaseConfig baseConfig() {
        MiniBaseConfig miniBaseConfig = new MiniBaseConfig();
        miniBaseConfig.setDataDir(this.miniBaseConfig.getDataDir());
        miniBaseConfig.setFlushMaxRetries(this.miniBaseConfig.getFlushMaxRetries());
        miniBaseConfig.setMaxDiskFiles(this.miniBaseConfig.getMaxDiskFiles());
        miniBaseConfig.setMaxMemstoreSize(this.miniBaseConfig.getMaxMemstoreSize());
        miniBaseConfig.setMaxThreadPoolSize(this.miniBaseConfig.getMaxThreadPoolSize());
        return miniBaseConfig;
    }
}
