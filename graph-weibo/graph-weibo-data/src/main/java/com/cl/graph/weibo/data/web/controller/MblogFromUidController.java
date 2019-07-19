package com.cl.graph.weibo.data.web.controller;

import com.cl.graph.weibo.data.manager.CsvFile;
import com.cl.graph.weibo.data.manager.CsvFileManager;
import com.cl.graph.weibo.data.service.MblogFromUidService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;

/**
 * @author yejianyu
 * @date 2019/7/17
 */
@RequestMapping(value = "/blog")
@RestController
public class MblogFromUidController {

    private final MblogFromUidService mblogFromUidService;
    private final CsvFileManager csvFileManager;

    public MblogFromUidController(MblogFromUidService mblogFromUidService, CsvFileManager csvFileManager) {
        this.mblogFromUidService = mblogFromUidService;
        this.csvFileManager = csvFileManager;
    }

    @RequestMapping(value = "/initRetweet")
    public String initRetweetEdge(@RequestParam(name = "resultPath") String resultPath,
                                  @RequestParam(name = "startDate") String startDate,
                                  @RequestParam(name = "endDate") String endDate) {
        mblogFromUidService.genRetweetFileByDate(resultPath, startDate, endDate);
        return "success";
    }

    @RequestMapping(value = "/initRetweetDetail")
    public String initRetweetEdge2(@RequestParam(name = "resultPath") String resultPath,
                                   @RequestParam(name = "startDate") String startDate,
                                   @RequestParam(name = "endDate") String endDate) {
        mblogFromUidService.genRetweetDetailFileByDate(resultPath, startDate, endDate);
        return "success";
    }

    @RequestMapping(value = "/mergeFile")
    public String initUserVertex(@RequestParam(name = "oriFilePath") String oriFilePath){
        File file = new File(oriFilePath);
        String[] files = file.list();
        String[] header = {"from", "to", "mid", "createTime"};
        CsvFile csvFile = new CsvFile(oriFilePath, file.getName(), header);
        csvFileManager.mergeFile(csvFile, files);
        return "success";
    }
}
