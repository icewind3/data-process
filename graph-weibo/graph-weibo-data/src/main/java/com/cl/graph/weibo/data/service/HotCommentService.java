package com.cl.graph.weibo.data.service;

import com.cl.graph.weibo.core.constant.CommonConstants;
import com.cl.graph.weibo.core.constant.FileSuffixConstants;
import com.cl.graph.weibo.data.dto.CommentDTO;
import com.cl.graph.weibo.data.entity.UserInfo;
import com.cl.graph.weibo.data.entity.HotComment;
import com.cl.graph.weibo.data.manager.RedisDataManager;
import com.cl.graph.weibo.data.mapper.marketing.HotCommentMapper;
import com.cl.graph.weibo.data.util.CsvFileHelper;
import com.cl.graph.weibo.data.util.UserInfoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Slf4j
@Service
public class HotCommentService {

    @Autowired
    private RedisDataManager redisDataManager;

    @Resource
    private HotCommentMapper hotCommentMapper;

    private static final String HOT_COMMENT_FILE_DIC = "comment";
    private static final String RELATION_COMMENT = "_comment_";
    private static final String[] HEADER_COMMENT = {"from", "to", "mid", "createTime"};

    private static final DateTimeFormatter DATE_TIME_FORMATTER_INPUT = DateTimeFormatter
            .ofPattern("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);
    private static final DateTimeFormatter DATE_TIME_FORMATTER_OUTPUT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public int genCommentFile(String resultPath, String tableSuffix) {

        int pageSize = 100000;
        long maxCount;
        long startTime = System.currentTimeMillis();
        log.info("开始处理hot_comment_{}", tableSuffix);
        try {
            maxCount = hotCommentMapper.count(tableSuffix);
        } catch (InvalidDataAccessResourceUsageException e) {
            log.warn("表hot_comment_{}不存在", tableSuffix);
            return 0;
        }
        log.info("统计hot_comment_{}的总数为{}, 耗时{}ms", tableSuffix, maxCount,
                System.currentTimeMillis() - startTime);
        int count = 0;
        int pageNum = 1;
        Map<String, List<CommentDTO>> map = new HashMap<>(6);
        while (count < maxCount) {
            List<HotComment> commentList = hotCommentMapper.findAll(tableSuffix, pageNum, pageSize);
            wrapCommentDataToMap(commentList, map);
            count += commentList.size();
            pageNum++;
            log.info("hot_comment_{}已处理:{}/{}, 已耗时{}ms", tableSuffix, count, maxCount,
                    System.currentTimeMillis() - startTime);
        }
        File fileDic = new File(resultPath + File.separator + HOT_COMMENT_FILE_DIC);
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
        log.info("hot_comment_{}已处理完成，实际生成{}条边数据, 共耗时{}ms", tableSuffix, resultCount[0],
                System.currentTimeMillis() - startTime);
        return resultCount[0];
    }

    private void wrapCommentDataToMap(List<HotComment> comments, Map<String, List<CommentDTO>> map) {
        comments.forEach(hotComment -> {

            String fromUid = hotComment.getUid();
            UserInfo fromUserInfo = redisDataManager.getUserInfoViaCache(fromUid);
            if (fromUserInfo == null) {
                return;
            }
            String mid = hotComment.getMid();
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
                String relationship = fromUserType + RELATION_COMMENT + toUserType;
                CommentDTO commentDTO = new CommentDTO(fromUid, toUid);
                commentDTO.setMid(mid);
                TemporalAccessor parse = DATE_TIME_FORMATTER_INPUT.parse(hotComment.getCreateTime());
                String createTime = DATE_TIME_FORMATTER_OUTPUT.format(parse);
                commentDTO.setCreateTime(createTime);
                if (map.containsKey(relationship)) {
                    map.get(relationship).add(commentDTO);
                } else {
                    List<CommentDTO> commentDTOList = new ArrayList<>();
                    commentDTOList.add(commentDTO);
                    map.put(relationship, commentDTOList);
                }
            }
        });
    }

    private void writeToCsvFile(List<CommentDTO> content, String filePath) throws IOException {
        try (CSVPrinter printer = CsvFileHelper.writer(filePath, HEADER_COMMENT)) {
            for (CommentDTO commentDTO : content) {
                printer.printRecord(commentDTO.getFromUid(), commentDTO.getToUid(), commentDTO.getMid(),
                        commentDTO.getCreateTime());
            }
        }
    }
}
