package com.cl.graph.weibo.data.service;

import com.alibaba.fastjson.JSON;
import com.cl.graph.weibo.core.exception.ServiceException;
import com.cl.graph.weibo.data.constant.SSDBConstants;
import com.cl.graph.weibo.data.entity.UserInfo;
import com.cl.graph.weibo.data.entity.BigVCategory;
import com.cl.graph.weibo.data.entity.FriendlyLinkTradeCompany;
import com.cl.graph.weibo.data.manager.RedisDataManager;
import com.cl.graph.weibo.data.mapper.weibo.BigVCategoryMapper;
import com.cl.graph.weibo.data.mapper.weibo.FriendlyLinkTradeCompanyMapper;
import com.cl.graph.weibo.data.util.CsvFileHelper;
import com.cl.graph.weibo.data.util.UserInfoUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author yejianyu
 * @date 2019/7/16
 */
@Slf4j
@Service
public class UserFileService {

    @Resource(name = "userFriendsFollowersRedisTemplate")
    private StringRedisTemplate userFriendsFollowersRedisTemplate;

    @Autowired
    private RedisDataManager redisDataManager;

    private static final String[] HEADER_USER_PERSONAL = {"ID", "昵称", "类型", "领域标签1", "领域标签2", "领域标签3",
            "PGRank1", "PGRank2", "PGRank3", "PGRank4"};
    private static final String[] HEADER__USER_ENTERPRISE = {"ID", "昵称", "领域标签1", "领域标签2", "领域标签3",
            "PGRank1", "PGRank2", "PGRank3", "PGRank4"};
    private static final String[] FRIENDLY_LINK_TRADE_COMPANY_SUFFIX = {"20190619", "20190618", "20190617"};

    @Resource
    private BigVCategoryMapper bigVCategoryMapper;

    @Resource
    private FriendlyLinkTradeCompanyMapper friendlyLinkTradeCompanyMapper;

    public void genUserFileByTraverse(String resultPath) throws ServiceException {
        String enterpriseFile = resultPath + File.separator + "blue_v.csv";
        String allPersonalFile = resultPath + File.separator + "personal_all.csv";
        String corePersonalFile = resultPath + File.separator + "personal_core.csv";
        String importantPersonalFile = resultPath + File.separator + "personal_important.csv";
        long startTime = System.currentTimeMillis();
        log.info("开始遍历userInfo生成节点");
        try (SSDB ssdb = SSDBs.pool("192.168.2.16", 8889, 1000 * 100, null);
             CSVPrinter enterpriseWriter = CsvFileHelper.writer(enterpriseFile, HEADER__USER_ENTERPRISE);
             CSVPrinter allPersonalWriter = CsvFileHelper.writer(allPersonalFile, HEADER_USER_PERSONAL);
             CSVPrinter corePersonalWriter = CsvFileHelper.writer(corePersonalFile, HEADER_USER_PERSONAL);
             CSVPrinter importantPersonalWriter = CsvFileHelper.writer(importantPersonalFile, HEADER_USER_PERSONAL)) {
            String start = StringUtils.EMPTY;
            String end = StringUtils.EMPTY;
            final long[] max = {0};
            long index = 0;
            int stepSize = 1000;
            while (true) {
                if (max[0] != 0) {
                    start = String.valueOf(max[0]);
                }
                Response response = ssdb.hscan(SSDBConstants.SSDB_USER_INFO_KEY, start, end, stepSize);
                Map<String, String> map = response.mapString();
                map.forEach((k, v) -> {
                    UserInfo userInfo = JSON.parseObject(v, UserInfo.class);
                    max[0] = Long.parseLong(userInfo.getUid());
                    String tag = getDomainTag(userInfo);
                    try {
                        if (userInfo.isBlueV()) {
                            enterpriseWriter.printRecord(userInfo.getUid(), userInfo.getName(), tag);
                            return;
                        }
                        String personalUserType = UserInfoUtils.getPersonalUserType(userInfo);
                        if (userInfo.isCoreUser()) {
                            corePersonalWriter.printRecord(userInfo.getUid(), userInfo.getName(), personalUserType, tag);
                        }
                        if (userInfo.isImportantUser()) {
                            importantPersonalWriter.printRecord(userInfo.getUid(), userInfo.getName(), personalUserType, tag);
                        }
                        allPersonalWriter.printRecord(userInfo.getUid(), userInfo.getName(), personalUserType, tag);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                index++;
                if (index % 10 == 0) {
                    log.info("已处理index={}, step={}, 已耗时{}ms", index, stepSize, System.currentTimeMillis() - startTime);
                }
                if (map.size() < stepSize) {
                    break;
                }
                map.clear();
            }
        } catch (IOException e) {
            log.error("遍历userInfo出错", e);
            throw new ServiceException("遍历userInfo出错", e);
        }
        log.info("已完成节点文件生成，共耗时{}s", (System.currentTimeMillis() - startTime) / 1000);
    }

    public long genUserFile(String key, String resultPath) {
        Set<Object> ids = userFriendsFollowersRedisTemplate.opsForHash().keys(key);
        if (ids == null || ids.size() == 0) {
            return 0;
        }

        File file = new File(resultPath);
        if (file.isDirectory()) {
            resultPath = resultPath + File.separator + key + ".csv";
        }
        try (CSVPrinter writer = CsvFileHelper.writer(resultPath, HEADER__USER_ENTERPRISE)) {
            ids.forEach(uid -> {
                try {
                    UserInfo userInfo = redisDataManager.getUserInfo((String) uid);
                    writer.printRecord(uid, userInfo.getName(), getDomainTag(userInfo));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ids.size();
    }

    private String getDomainTag(UserInfo userInfo) {
        String uid = userInfo.getUid();
        if (userInfo.isPersonalBigV()) {
            List<BigVCategory> categoryByUid = bigVCategoryMapper.findCategoryByUid(uid);
            if (categoryByUid != null && !categoryByUid.isEmpty()) {
                return categoryByUid.get(0).getMinNavigation();
            }
            List<BigVCategory> peopleByUid = bigVCategoryMapper.findPeopleByUid(uid);
            if (peopleByUid != null && !peopleByUid.isEmpty()) {
                return peopleByUid.get(0).getMinNavigation();
            }
            return StringUtils.EMPTY;
        }
        if (userInfo.isBlueV()) {
            for (String suffix : FRIENDLY_LINK_TRADE_COMPANY_SUFFIX) {
                FriendlyLinkTradeCompany blueV = friendlyLinkTradeCompanyMapper.findByUid(uid, suffix);
                if (blueV != null) {
                    return blueV.getTrade();
                }
            }
        }
        return StringUtils.EMPTY;
    }
}
