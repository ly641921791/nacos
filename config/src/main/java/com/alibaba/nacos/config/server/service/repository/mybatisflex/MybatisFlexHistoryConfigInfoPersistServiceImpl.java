package com.alibaba.nacos.config.server.service.repository.mybatisflex;

import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.configuration.ConditionOnExternalStorage;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.model.ConfigHistoryInfo;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.repository.HistoryConfigInfoPersistService;
import com.alibaba.nacos.config.server.service.repository.PaginationHelper;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.HistoryConfigInfoEntity;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.mapper.HistoryConfigInfoEntityMapper;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.mybatisflex.core.query.QueryMethods;
import com.mybatisflex.core.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.nacos.api.common.Constants.GROUP;
import static com.alibaba.nacos.config.server.service.repository.mybatisflex.MybatisFlexUtils.concatEq;
import static com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoMapper.TENANT;

/**
 * ExternalHistoryConfigInfoPersistServiceImpl.
 *
 * @author lixiaoshuang
 */
@SuppressWarnings(value = {"PMD.MethodReturnWrapperTypeRule", "checkstyle:linelength"})
@Conditional(value = ConditionOnExternalStorage.class)
@Service
public class MybatisFlexHistoryConfigInfoPersistServiceImpl implements HistoryConfigInfoPersistService {

    @Autowired
    private HistoryConfigInfoEntityMapper historyConfigInfoEntityMapper;

    @Override
    @Deprecated
    public <E> PaginationHelper<E> createPaginationHelper() {
        return null;
    }

    @Override
    public List<ConfigInfo> convertDeletedConfig(List<Map<String, Object>> list) {
        List<ConfigInfo> configs = new ArrayList<>();
        for (Map<String, Object> map : list) {
            String dataId = (String) map.get("data_id");
            String group = (String) map.get("group_id");
            String tenant = (String) map.get("tenant_id");
            ConfigInfo config = new ConfigInfo();
            config.setDataId(dataId);
            config.setGroup(group);
            config.setTenant(tenant);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public void insertConfigHistoryAtomic(long id, ConfigInfo configInfo, String srcIp, String srcUser,
                                          final Timestamp time, String ops) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
        final String md5Tmp = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
        String encryptedDataKey = StringUtils.isBlank(configInfo.getEncryptedDataKey()) ? StringUtils.EMPTY
                : configInfo.getEncryptedDataKey();

        try {
            HistoryConfigInfoEntity historyConfigInfoEntity = new HistoryConfigInfoEntity();
            historyConfigInfoEntity.setId(id);
            historyConfigInfoEntity.setDataId(configInfo.getDataId());
            historyConfigInfoEntity.setGroupId(configInfo.getGroup());
            historyConfigInfoEntity.setTenantId(tenantTmp);
            historyConfigInfoEntity.setAppName(appNameTmp);
            historyConfigInfoEntity.setContent(configInfo.getContent());
            historyConfigInfoEntity.setMd5(md5Tmp);
            historyConfigInfoEntity.setSrcIp(srcIp);
            historyConfigInfoEntity.setSrcUser(srcUser);
            historyConfigInfoEntity.setGmtModified(time.toLocalDateTime());
            historyConfigInfoEntity.setOpType(ops);
            historyConfigInfoEntity.setEncryptedDataKey(encryptedDataKey);
            historyConfigInfoEntityMapper.insert(historyConfigInfoEntity);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public void removeConfigHistory(final Timestamp startTime, final int limitSize) {
        QueryWrapper queryWrapper = QueryWrapper.create();
        queryWrapper.lt(HistoryConfigInfoEntity::getGmtModified, startTime.toLocalDateTime());
        queryWrapper.limit(limitSize);
        historyConfigInfoEntityMapper.deleteByQuery(queryWrapper);
    }

    @Override
    public List<ConfigInfo> findDeletedConfig(final Timestamp startTime, final Timestamp endTime) {
        try {
            QueryWrapper queryWrapper = QueryWrapper.create();
            queryWrapper.select(QueryMethods.distinct(HistoryConfigInfoEntity::getDataId));
            queryWrapper.select(HistoryConfigInfoEntity::getGroupId).as(GROUP);
            queryWrapper.select(HistoryConfigInfoEntity::getTenantId).as(TENANT);
            queryWrapper.eq(HistoryConfigInfoEntity::getOpType, "D");
            queryWrapper.between(HistoryConfigInfoEntity::getGmtModified, startTime.toLocalDateTime(), endTime.toLocalDateTime());
            return historyConfigInfoEntityMapper.selectListByQueryAs(queryWrapper, ConfigInfo.class);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public Page<ConfigHistoryInfo> findConfigHistory(String dataId, String group, String tenant, int pageNo,
                                                     int pageSize) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        QueryWrapper queryWrapper = QueryWrapper.create();
        queryWrapper.select(HistoryConfigInfoEntity::getNid);
        queryWrapper.select(HistoryConfigInfoEntity::getDataId);
        queryWrapper.select(HistoryConfigInfoEntity::getGroupId).as(GROUP);
        queryWrapper.select(HistoryConfigInfoEntity::getTenantId).as(TENANT);
        queryWrapper.select(HistoryConfigInfoEntity::getAppName);
        queryWrapper.select(HistoryConfigInfoEntity::getSrcIp);
        queryWrapper.select(HistoryConfigInfoEntity::getSrcUser);
        queryWrapper.select(HistoryConfigInfoEntity::getOpType);
        queryWrapper.select(HistoryConfigInfoEntity::getGmtCreate);
        queryWrapper.select(HistoryConfigInfoEntity::getGmtModified);
        concatEq(queryWrapper, HistoryConfigInfoEntity::getDataId, dataId);
        concatEq(queryWrapper, HistoryConfigInfoEntity::getGroupId, group);
        concatEq(queryWrapper, HistoryConfigInfoEntity::getTenantId, tenantTmp);
        queryWrapper.orderBy(HistoryConfigInfoEntity::getNid, false);

        Page<ConfigHistoryInfo> page = null;
        try {
            page = MybatisFlexUtils.convertPage(historyConfigInfoEntityMapper.paginateAs(pageNo, pageSize, queryWrapper, ConfigHistoryInfo.class));
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[list-config-history] error, dataId:{}, group:{}", new Object[]{dataId, group},
                    e);
            throw e;
        }
        return page;
    }

    @Override
    public ConfigHistoryInfo detailConfigHistory(Long nid) {
        QueryWrapper queryWrapper = QueryWrapper.create();
        queryWrapper.select(HistoryConfigInfoEntity::getNid);
        queryWrapper.select(HistoryConfigInfoEntity::getDataId);
        queryWrapper.select(HistoryConfigInfoEntity::getGroupId).as(GROUP);
        queryWrapper.select(HistoryConfigInfoEntity::getTenantId).as(TENANT);
        queryWrapper.select(HistoryConfigInfoEntity::getAppName);
        queryWrapper.select(HistoryConfigInfoEntity::getContent);
        queryWrapper.select(HistoryConfigInfoEntity::getMd5);
        queryWrapper.select(HistoryConfigInfoEntity::getSrcIp);
        queryWrapper.select(HistoryConfigInfoEntity::getSrcUser);
        queryWrapper.select(HistoryConfigInfoEntity::getOpType);
        queryWrapper.select(HistoryConfigInfoEntity::getGmtCreate);
        queryWrapper.select(HistoryConfigInfoEntity::getGmtModified);
        queryWrapper.select(HistoryConfigInfoEntity::getEncryptedDataKey);
        queryWrapper.eq(HistoryConfigInfoEntity::getNid, nid);
        try {
            return historyConfigInfoEntityMapper.selectOneByQueryAs(queryWrapper, ConfigHistoryInfo.class);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[detail-config-history] error, nid:{}", new Object[]{nid}, e);
            throw e;
        }
    }

    @Override
    public ConfigHistoryInfo detailPreviousConfigHistory(Long id) {
        QueryWrapper queryWrapper = QueryWrapper.create();
        queryWrapper.select(HistoryConfigInfoEntity::getNid);
        queryWrapper.select(HistoryConfigInfoEntity::getDataId);
        queryWrapper.select(HistoryConfigInfoEntity::getGroupId).as(GROUP);
        queryWrapper.select(HistoryConfigInfoEntity::getTenantId).as(TENANT);
        queryWrapper.select(HistoryConfigInfoEntity::getAppName);
        queryWrapper.select(HistoryConfigInfoEntity::getContent);
        queryWrapper.select(HistoryConfigInfoEntity::getMd5);
        queryWrapper.select(HistoryConfigInfoEntity::getSrcIp);
        queryWrapper.select(HistoryConfigInfoEntity::getSrcUser);
        queryWrapper.select(HistoryConfigInfoEntity::getOpType);
        queryWrapper.select(HistoryConfigInfoEntity::getGmtCreate);
        queryWrapper.select(HistoryConfigInfoEntity::getGmtModified);
        QueryWrapper subQueryWrapper = QueryWrapper.create();
        subQueryWrapper.select(QueryMethods.max(HistoryConfigInfoEntity::getNid));
        subQueryWrapper.eq(HistoryConfigInfoEntity::getId, id);
        queryWrapper.eq(HistoryConfigInfoEntity::getNid, subQueryWrapper);
        try {
            return historyConfigInfoEntityMapper.selectOneByQueryAs(queryWrapper, ConfigHistoryInfo.class);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[detail-previous-config-history] error, id:{}", new Object[]{id}, e);
            throw e;
        }
    }

    @Override
    @Deprecated
    public int findConfigHistoryCountByTime(final Timestamp startTime) {
        return 0;
    }
}
