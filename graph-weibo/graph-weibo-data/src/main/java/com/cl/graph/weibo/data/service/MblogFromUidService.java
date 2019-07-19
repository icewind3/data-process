package com.cl.graph.weibo.data.service;

import com.cl.graph.weibo.core.constant.CommonConstants;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.data.dto.RetweetDTO;
import com.cl.graph.weibo.data.entity.MblogFromUid;
import com.cl.graph.weibo.data.entity.UserInfo;
import com.cl.graph.weibo.data.manager.RedisDataManager;
import com.cl.graph.weibo.data.mapper.marketing.MblogFromUidMapper;
import com.cl.graph.weibo.data.util.CsvFileHelper;
import com.cl.graph.weibo.data.util.UserInfoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Slf4j
@Service
public class MblogFromUidService {

    @Autowired
    private RedisDataManager redisDataManager;

    @Resource(name = "commonThreadPoolTaskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    @Resource
    private MblogFromUidMapper mblogFromUidMapper;

    private static final String RETWEET_FILE_DIC = "retweet";
    private static final String RELATION_RETWEET = "_retweet_";
    private static final String[] HEADER_RETWEET = {"from", "to", "mid", "createTime"};

    public void genRetweetFileByDate(String resultPath, String startDate, String endDate) {
        List<String> dateList = getDateList(startDate, endDate);
        int indexSize = 10;
        CountDownLatch countDownLatch = new CountDownLatch(dateList.size() * indexSize);
        for (String date : dateList) {
            for (int i = 0; i < indexSize; i++) {
                final String suffix = date + "_" + i;
                taskExecutor.execute(() -> {
                    try {
                        genRetweetFile(resultPath, suffix);
                    } finally {
                        countDownLatch.countDown();
                    }
                });
            }
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void genRetweetDetailFileByDate(String resultPath, String startDate, String endDate) {
        List<String> dateList = getDateList(startDate, endDate);
        CountDownLatch countDownLatch = new CountDownLatch(dateList.size());
        for (String date : dateList) {
            final String suffix = "detail_" + date;
            taskExecutor.execute(() -> {
                try {
                    genRetweetFile(resultPath, suffix);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void genRetweetFile(String resultPath, String tableSuffix) {
        int pageSize = 100000;
        long maxCount;
        long startTime = System.currentTimeMillis();
        log.info("开始处理mblog_from_uid_{}", tableSuffix);
        try {
            maxCount = mblogFromUidMapper.countRetweet(tableSuffix);
        } catch (InvalidDataAccessResourceUsageException e) {
            String message = e.getMessage();
            if (StringUtils.containsIgnoreCase(message, "Table")
                    && StringUtils.containsIgnoreCase(message, "doesn't exist")) {
                log.warn("表mblog_from_uid_{}不存在", tableSuffix);
            } else {
                log.error("处理表mblog_from_uid_" + tableSuffix + "出错", e);
            }
            return;
        }
        log.info("统计mblog_from_uid_{}的转发数为{}, 耗时{}ms", tableSuffix, maxCount,
                System.currentTimeMillis() - startTime);
        int count = 0;
        int pageNum = 1;
        Map<String, List<RetweetDTO>> map = new HashMap<>(6);
        while (count < maxCount) {
            List<MblogFromUid> blogRetweetList = mblogFromUidMapper.findAllRetweet(tableSuffix, pageNum, pageSize);
            wrapRetweetData(blogRetweetList, map);
            count += blogRetweetList.size();
            pageNum++;
            log.info("mblog_from_uid_{}已处理:{}/{}, 已耗时{}ms", tableSuffix, count, maxCount,
                    System.currentTimeMillis() - startTime);
        }
        File fileDic = new File(resultPath + File.separator + RETWEET_FILE_DIC);
        fileDic.mkdir();
        final int[] resultCount = {0};
        map.forEach((key, commentList) -> {
            resultCount[0] += commentList.size();
            String commentFileDicName = fileDic.getPath() + File.separator + key;
            File commentFileDic = new File(commentFileDicName);
            commentFileDic.mkdir();
            String filePath = commentFileDicName + File.separator + key + CommonConstants.UNDERLINE + tableSuffix
                    + FileSuffixConstants.CSV;
            try {
                writeToCsvFile(commentList, filePath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        log.info("mblog_from_uid_{}已处理完成，实际生成{}条边数据, 共耗时{}ms", tableSuffix, resultCount[0],
                System.currentTimeMillis() - startTime);
    }

    private void wrapRetweetData(List<MblogFromUid> retweetList, Map<String, List<RetweetDTO>> map) {
        retweetList.forEach(mblogFromUid -> {

            String fromUid = mblogFromUid.getUid();
            UserInfo fromUserInfo = redisDataManager.getUserInfoViaCache(fromUid);
            if (fromUserInfo == null) {
                return;
            }
            String pid = mblogFromUid.getPid();
            String mid;
            if (StringUtils.isNotBlank(pid)) {
                mid = pid;
            } else {
                mid = mblogFromUid.getRetweetedMid();
            }
            String toUid = redisDataManager.getUidByMid(mid);
            if (StringUtils.isBlank(toUid)) {
                return;
            }
            UserInfo toUserInfo = redisDataManager.getUserInfoViaCache(toUid);
            if (toUserInfo == null || !toUserInfo.isCoreUser()) {
                return;
            }
            String fromUserType = UserInfoUtils.getUserType(fromUserInfo);
            String toUserType = UserInfoUtils.getUserType(toUserInfo);
            if (StringUtils.isNotBlank(fromUserType) && StringUtils.isNotBlank(toUserType)) {
                String relationship = fromUserType + RELATION_RETWEET + toUserType;
                RetweetDTO retweetDTO = new RetweetDTO(fromUid, toUid);
                retweetDTO.setMid(mid);
                retweetDTO.setCreateTime(mblogFromUid.getCreateTime());
                if (map.containsKey(relationship)) {
                    map.get(relationship).add(retweetDTO);
                } else {
                    List<RetweetDTO> list = new ArrayList<>();
                    list.add(retweetDTO);
                    map.put(relationship, list);
                }
            }
        });
    }

    private void writeToCsvFile(List<RetweetDTO> content, String filePath) throws IOException {
        try (CSVPrinter printer = CsvFileHelper.writer(filePath, HEADER_RETWEET)) {
            for (RetweetDTO retweetDTO : content) {
                printer.printRecord(retweetDTO.getFromUid(), retweetDTO.getToUid(), retweetDTO.getMid(),
                        retweetDTO.getCreateTime());
            }
        }
    }

    private List<String> getDateList(String startDate, String endDate) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date fromDate;
        Date toDate;
        try {
            fromDate = simpleDateFormat.parse(startDate);
            toDate = simpleDateFormat.parse(endDate);
        } catch (ParseException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
        List<String> dateList = new ArrayList<>();
        while (!fromDate.after(toDate)) {
            dateList.add(simpleDateFormat.format(fromDate));
            fromDate = DateUtils.addDays(fromDate, 1);
        }
        return dateList;
    }
}
