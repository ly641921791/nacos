package com.alibaba.nacos.config.server.service.repository.mybatisflex;

import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.configuration.ConditionOnExternalStorage;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.ConfigInfoBetaWrapper;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoBetaPersistService;
import com.alibaba.nacos.config.server.service.repository.PaginationHelper;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.ConfigInfoBetaEntity;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.table.ConfigInfoBetaEntityTableDef;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.mapper.ConfigInfoBetaEntityMapper;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.mybatisflex.core.query.QueryCondition;
import com.mybatisflex.core.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.alibaba.nacos.config.server.service.repository.mybatisflex.MybatisFlexUtils.concatEq;
import static com.alibaba.nacos.config.server.service.repository.mybatisflex.MybatisFlexUtils.convertPage;

/**
 * ExternalConfigInfoBetaPersistServiceImpl.
 *
 * @author lixiaoshuang
 */
@SuppressWarnings(value = {"PMD.MethodReturnWrapperTypeRule", "checkstyle:linelength"})
@Conditional(value = ConditionOnExternalStorage.class)
@Service
public class MybatisFlexConfigInfoBetaPersistServiceImpl implements ConfigInfoBetaPersistService {

    @Autowired
    protected TransactionTemplate tjt;

    @Autowired
    private ConfigInfoBetaEntityMapper configInfoBetaEntityMapper;

    @Override
    @Deprecated
    public <E> PaginationHelper<E> createPaginationHelper() {
        return null;
    }

    @Override
    public void addConfigInfo4Beta(ConfigInfo configInfo, String betaIps, String srcIp, String srcUser, Timestamp time,
                                   boolean notify) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
        String md5 = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
        String encryptedDataKey = StringUtils.isBlank(configInfo.getEncryptedDataKey()) ? StringUtils.EMPTY
                : configInfo.getEncryptedDataKey();
        try {
            ConfigInfoBetaEntity configInfoBetaEntity = new ConfigInfoBetaEntity();
            configInfoBetaEntity.setDataId(configInfo.getDataId());
            configInfoBetaEntity.setGroupId(configInfo.getGroup());
            configInfoBetaEntity.setTenantId(tenantTmp);
            configInfoBetaEntity.setAppName(appNameTmp);
            configInfoBetaEntity.setContent(configInfo.getContent());
            configInfoBetaEntity.setMd5(md5);
            configInfoBetaEntity.setBetaIps(betaIps);
            configInfoBetaEntity.setSrcIp(srcIp);
            configInfoBetaEntity.setSrcUser(srcUser);
            configInfoBetaEntity.setGmtCreate(LocalDateTime.ofInstant(time.toInstant(), ZoneId.systemDefault()));
            configInfoBetaEntity.setGmtModified(LocalDateTime.ofInstant(time.toInstant(), ZoneId.systemDefault()));
            configInfoBetaEntity.setEncryptedDataKey(encryptedDataKey);
            configInfoBetaEntityMapper.insert(configInfoBetaEntity);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public void insertOrUpdateBeta(final ConfigInfo configInfo, final String betaIps, final String srcIp,
                                   final String srcUser, final Timestamp time, final boolean notify) {
        try {
            addConfigInfo4Beta(configInfo, betaIps, srcIp, null, time, notify);
        } catch (DataIntegrityViolationException ive) { // Unique constraint conflict
            updateConfigInfo4Beta(configInfo, betaIps, srcIp, null, time, notify);
        }
    }

    @Override
    public boolean insertOrUpdateBetaCas(final ConfigInfo configInfo, final String betaIps, final String srcIp,
                                         final String srcUser, final Timestamp time, final boolean notify) {
        try {
            addConfigInfo4Beta(configInfo, betaIps, srcIp, null, time, notify);
            return true;
        } catch (DataIntegrityViolationException ive) { // Unique constraint conflict
            return updateConfigInfo4BetaCas(configInfo, betaIps, srcIp, null, time, notify);
        }
    }

    @Override
    public void removeConfigInfo4Beta(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        tjt.execute(status -> {
            try {
                ConfigInfo configInfo = findConfigInfo4Beta(dataId, group, tenant);
                if (configInfo != null) {
                    QueryWrapper queryWrapper = QueryWrapper.create();
                    concatEq(queryWrapper, ConfigInfoBetaEntity::getDataId, dataId);
                    concatEq(queryWrapper, ConfigInfoBetaEntity::getGroupId, group);
                    concatEq(queryWrapper, ConfigInfoBetaEntity::getTenantId, tenantTmp);
                    configInfoBetaEntityMapper.deleteByQuery(queryWrapper);
                }
            } catch (CannotGetJdbcConnectionException e) {
                LogUtil.FATAL_LOG.error("[db-error] " + e, e);
                throw e;
            }
            return Boolean.TRUE;
        });
    }

    @Override
    public void updateConfigInfo4Beta(ConfigInfo configInfo, String betaIps, String srcIp, String srcUser,
                                      Timestamp time, boolean notify) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
        String md5 = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
        String encryptedDataKey = StringUtils.isBlank(configInfo.getEncryptedDataKey()) ? StringUtils.EMPTY
                : configInfo.getEncryptedDataKey();
        try {
            ConfigInfoBetaEntity configInfoBetaEntity = new ConfigInfoBetaEntity();
            configInfoBetaEntity.setContent(configInfo.getContent());
            configInfoBetaEntity.setMd5(md5);
            configInfoBetaEntity.setBetaIps(betaIps);
            configInfoBetaEntity.setSrcIp(srcIp);
            configInfoBetaEntity.setSrcUser(srcUser);
            configInfoBetaEntity.setGmtModified(LocalDateTime.ofInstant(time.toInstant(), ZoneId.systemDefault()));
            configInfoBetaEntity.setAppName(appNameTmp);
            configInfoBetaEntity.setEncryptedDataKey(encryptedDataKey);

            QueryWrapper queryWrapper = QueryWrapper.create();
            concatEq(queryWrapper, ConfigInfoBetaEntity::getDataId, configInfo.getDataId());
            concatEq(queryWrapper, ConfigInfoBetaEntity::getGroupId, configInfo.getGroup());
            concatEq(queryWrapper, ConfigInfoBetaEntity::getTenantId, tenantTmp);
            configInfoBetaEntityMapper.updateByQuery(configInfoBetaEntity, queryWrapper);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public boolean updateConfigInfo4BetaCas(ConfigInfo configInfo, String betaIps, String srcIp, String srcUser,
                                            Timestamp time, boolean notify) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
        String md5 = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
        try {
            ConfigInfoBetaEntity configInfoBetaEntity = new ConfigInfoBetaEntity();
            configInfoBetaEntity.setContent(configInfo.getContent());
            configInfoBetaEntity.setMd5(md5);
            configInfoBetaEntity.setBetaIps(betaIps);
            configInfoBetaEntity.setSrcIp(srcIp);
            configInfoBetaEntity.setSrcUser(srcUser);
            configInfoBetaEntity.setGmtModified(LocalDateTime.ofInstant(time.toInstant(), ZoneId.systemDefault()));
            configInfoBetaEntity.setAppName(appNameTmp);

            QueryWrapper queryWrapper = QueryWrapper.create();
            concatEq(queryWrapper, ConfigInfoBetaEntity::getDataId, configInfo.getDataId());
            concatEq(queryWrapper, ConfigInfoBetaEntity::getGroupId, configInfo.getGroup());
            concatEq(queryWrapper, ConfigInfoBetaEntity::getTenantId, tenantTmp);
            QueryCondition queryCondition = QueryCondition.createEmpty()
                    .or(ConfigInfoBetaEntityTableDef.CONFIG_INFO_BETA_ENTITY.MD5.eq(configInfo.getMd5()))
                    .or(ConfigInfoBetaEntityTableDef.CONFIG_INFO_BETA_ENTITY.MD5.isNull())
                    .or(ConfigInfoBetaEntityTableDef.CONFIG_INFO_BETA_ENTITY.MD5.eq(""));
            queryWrapper.and(queryCondition);

            return configInfoBetaEntityMapper.updateByQuery(configInfoBetaEntity, queryWrapper) > 0;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public ConfigInfoBetaWrapper findConfigInfo4Beta(final String dataId, final String group, final String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        try {
            QueryWrapper queryWrapper = QueryWrapper.create()
                    .select(ConfigInfoBetaEntity::getId)
                    .select(ConfigInfoBetaEntity::getDataId)
                    .select(ConfigInfoBetaEntity::getGroupId)
                    .select(ConfigInfoBetaEntity::getTenantId)
                    .select(ConfigInfoBetaEntity::getAppName)
                    .select(ConfigInfoBetaEntity::getContent)
                    .select(ConfigInfoBetaEntity::getBetaIps)
                    .select(ConfigInfoBetaEntity::getEncryptedDataKey);
            concatEq(queryWrapper, ConfigInfoBetaEntity::getDataId, dataId);
            concatEq(queryWrapper, ConfigInfoBetaEntity::getGroupId, group);
            concatEq(queryWrapper, ConfigInfoBetaEntity::getTenantId, tenantTmp);
            return configInfoBetaEntityMapper.selectOneByQueryAs(queryWrapper, ConfigInfoBetaWrapper.class);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null.
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public int configInfoBetaCount() {
        return (int) configInfoBetaEntityMapper.selectCountByCondition(QueryCondition.createEmpty());
    }

    @Override
    public Page<ConfigInfoBetaWrapper> findAllConfigInfoBetaForDumpAll(final int pageNo, final int pageSize) {
        try {
            return convertPage(configInfoBetaEntityMapper.paginateAs(pageNo, pageSize, QueryWrapper.create(), ConfigInfoBetaWrapper.class));
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }
}
