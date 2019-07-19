package com.cl.graph.weibo.data.service;

import com.alibaba.fastjson.JSON;
import com.cl.graph.weibo.core.constant.CommonConstants;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.data.dto.FollowingRelationshipDTO;
import com.cl.graph.weibo.data.entity.UserInfo;
import com.cl.graph.weibo.data.manager.RedisDataManager;
import com.cl.graph.weibo.data.util.CsvFileHelper;
import com.cl.graph.weibo.data.util.UserInfoMap;
import com.cl.graph.weibo.data.util.UserInfoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author yejianyu
 * @date 2019/7/18
 */
@Slf4j
@Service
public class FollowingRelationshipService {

    @Autowired
    private RedisDataManager redisDataManager;

    @Resource(name = "followingThreadPoolTaskExecutor")
    private ThreadPoolTaskExecutor followingTaskExecutor;

    private static final String FOLLOWING_FILE_DIC = "following";
    private static final String RELATION_FOLLOWING = "_following_";
    private static final String[] HEADER_FOLLOWING = {"from", "to"};

    public void genFollowingRelationshipFile(String filePath, String resultPath) {
        List<String> filePathList = getAllFilePath(filePath);
        if (filePathList == null || filePathList.size() == 0){
            return;
        }
        if (filePathList.size() == 1) {
            genFollowingRelationshipFileFromOneFile(filePathList.get(0), resultPath);
        } else {
            CountDownLatch countDownLatch = new CountDownLatch(filePathList.size());
            filePathList.forEach(file -> {
                followingTaskExecutor.execute(() -> {
                    try {
                        genFollowingRelationshipFileFromOneFile(file, resultPath);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            });
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private List<String> getAllFilePath(String filePath) {
        File oriFile = new File(filePath);
        if (oriFile.isDirectory()) {
            String[] files = oriFile.list();
            if (files == null || files.length == 0) {
                return Collections.emptyList();
            }
            List<String> fileList = new ArrayList<>();
            for (String file : files) {
                fileList.addAll(getAllFilePath(filePath + File.separator + file));
            }
            return fileList;
        }
        return Collections.singletonList(filePath);
    }

    private void genFollowingRelationshipFileFromOneFile(String filePath, String resultPath) {
        log.info("开始处理文件{}", filePath);
        File readFile = new File(filePath);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(readFile),
                StandardCharsets.UTF_8))) {
            int countEdge = 0;
            int infoThresholdCount = 5000;
            String line;
            Map<String, List<FollowingRelationshipDTO>> map = new HashMap<>(16);
            while ((line = br.readLine()) != null) {
                List<FollowingRelationshipDTO> relationshipList = JSON.parseArray(line, FollowingRelationshipDTO.class);
                for (FollowingRelationshipDTO followingRelationship : relationshipList) {
                    Long master = followingRelationship.getTo();
                    UserInfo toUserInfo = redisDataManager.getUserInfoViaCache(String.valueOf(master));
                    if (toUserInfo == null || !toUserInfo.isImportantUser()) {
                        continue;
                    }
                    Long slave = followingRelationship.getFrom();
                    UserInfo fromUserInfo = redisDataManager.getUserInfoViaCache(String.valueOf(slave));
                    String fromUserType = UserInfoUtils.getUserType(fromUserInfo);
                    String toUserType = UserInfoUtils.getUserType(toUserInfo);
                    if (StringUtils.isNotBlank(fromUserType) && StringUtils.isNotBlank(toUserType)) {
                        String relationship = fromUserType + RELATION_FOLLOWING + toUserType;
                        if (map.containsKey(relationship)) {
                            map.get(relationship).add(followingRelationship);
                        } else {
                            List<FollowingRelationshipDTO> followingRelationshipList = new ArrayList<>();
                            followingRelationshipList.add(followingRelationship);
                            map.put(relationship, followingRelationshipList);
                        }
                    }
                }
                File fileDic = new File(resultPath + File.separator + FOLLOWING_FILE_DIC);
                fileDic.mkdir();
                countEdge += writeToFile(map, fileDic.getPath(), readFile.getName());
                if (countEdge >= infoThresholdCount) {
                    infoThresholdCount = (countEdge / 5000 + 1) * 5000;
                    log.info("处理文件{},已生成{}条边", filePath, countEdge);
                }
                map.clear();
            }
            log.info("处理文件{}完成, 共生成{}条边", filePath, countEdge);
        } catch (IOException e) {
            log.error("处理文件" + filePath + "出错", e);
        }
    }

    private int writeToFile(Map<String, List<FollowingRelationshipDTO>> map, String resultDic, String fileSuffix) {
        int[] resultCount = {0};
        map.forEach((key, followingList) -> {
            resultCount[0] += followingList.size();
            String followingFileDicName = resultDic + File.separator + key;
            File followingFileDic = new File(followingFileDicName);
            followingFileDic.mkdir();
            String filePath = followingFileDicName + File.separator + key + CommonConstants.UNDERLINE + fileSuffix
                    + FileSuffixConstants.CSV;
            try {
                writeToCsvFile(followingList, filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        return resultCount[0];
    }

    private void writeToCsvFile(List<FollowingRelationshipDTO> content, String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            try (CSVPrinter printer = CsvFileHelper.writer(filePath, true)) {
                for (FollowingRelationshipDTO followingRelationship : content) {
                    printer.printRecord(followingRelationship.getFrom(), followingRelationship.getTo());
                }
            }
        } else {
            try (CSVPrinter printer = CsvFileHelper.writer(filePath, HEADER_FOLLOWING)) {
                for (FollowingRelationshipDTO followingRelationship : content) {
                    printer.printRecord(followingRelationship.getFrom(), followingRelationship.getTo());
                }
            }
        }
    }
}
