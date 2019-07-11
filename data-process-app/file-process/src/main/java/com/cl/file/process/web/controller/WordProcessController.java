package com.cl.file.process.web.controller;

import com.cl.file.process.service.WordProcessService;
import com.cl.file.process.web.ResponseResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author yejianyu
 * @date 2019/7/4
 */
@RestController
@RequestMapping(value = "/word")
public class WordProcessController {

    private final WordProcessService wordProcessService;

    public WordProcessController(WordProcessService wordProcessService) {
        this.wordProcessService = wordProcessService;
    }

    @RequestMapping(value = "/countWord")
    public ResponseResult countWord(String wordFilePath, String countFilePath, Integer wordIndex, Integer countIndex) {
        wordProcessService.countWord(wordFilePath, countFilePath, wordIndex, countIndex);
        return ResponseResult.SUCCESS;
    }

    @RequestMapping(value = "/countWordByKey")
    public ResponseResult countWordByUid(@RequestParam String wordFilePath, @RequestParam String countFilePath,
                                         @RequestParam Integer keyIndex, @RequestParam Integer wordIndex,
                                         @RequestParam Integer countIndex) {
        wordProcessService.countWordByKey(wordFilePath, countFilePath, keyIndex, wordIndex, countIndex);
        return ResponseResult.SUCCESS;
    }
}
