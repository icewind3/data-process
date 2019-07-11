package com.cl.file.process.web.controller;

import com.cl.file.process.service.FileFilterService;
import com.cl.file.process.web.ResponseResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yejianyu
 * @date 2019/7/5
 */
@RestController
@RequestMapping(value = "/file")
public class FileFilterController {

    private final FileFilterService fileFilterService;

    public FileFilterController(FileFilterService fileFilterService) {
        this.fileFilterService = fileFilterService;
    }

    @RequestMapping(value = "/filterVertex")
    public ResponseResult filterVertexFile(@RequestParam String keyFilePath, @RequestParam String oriFilePath,
                             @RequestParam(defaultValue = "0") Integer filterIndex, @RequestParam String resultFilePath,
                                           @RequestParam(defaultValue = "true") Boolean needInclude) {
        fileFilterService.filterFileByOneKey(keyFilePath, oriFilePath, filterIndex, resultFilePath, needInclude);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/filterEdge")
    public ResponseResult filterEdgeFile(@RequestParam String keyFilePath, @RequestParam String oriFilePath,
                                     @RequestParam(defaultValue = "0") Integer fromIndex, @RequestParam(defaultValue = "1")
                                             Integer toIndex, @RequestParam String resultFilePath) {
        fileFilterService.filterEdgeFile(keyFilePath, oriFilePath, fromIndex, toIndex, resultFilePath);
        return ResponseResult.SUCCESS;
    }
}
