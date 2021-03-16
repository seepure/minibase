package org.apache.minibase.server.controller;

import org.apache.minibase.server.service.MiniBaseService;
import org.apache.minibase.server.vo.ResponseVO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "data")
public class DataController {
    @Autowired
    private MiniBaseService miniBaseService;

    @RequestMapping(value = "put", method = RequestMethod.POST)
    public ResponseVO put(@RequestParam(name = "key")String key, @RequestParam(name = "value")String value) {
        try {
            miniBaseService.put(key, value);
        } catch (Exception e) {
            return new ResponseVO(503, e.getMessage(), e.toString());
        }
        return new ResponseVO(200, "OK", null);
    }

    @RequestMapping(value = "get", method = RequestMethod.POST)
    public ResponseVO get(@RequestParam(name = "key")String key) {
        String value = null;
        try {
            value = miniBaseService.get(key);
        } catch (Exception e) {
            return new ResponseVO(503, e.getMessage(), e.toString());
        }
        return new ResponseVO(200, "OK", value);
    }
}
