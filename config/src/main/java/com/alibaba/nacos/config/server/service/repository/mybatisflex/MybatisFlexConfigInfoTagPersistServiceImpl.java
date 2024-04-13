package com.alibaba.nacos.config.server.service.repository.mybatisflex;

import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.configuration.ConditionOnExternalStorage;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.ConfigInfoTagWrapper;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoTagPersistService;
import com.alibaba.nacos.config.server.service.repository.PaginationHelper;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.ConfigInfoTagEntity;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.table.ConfigInfoTagEntityTableDef;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.mapper.ConfigInfoTagEntityMapper;
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

import static com.alibaba.nacos.api.common.Constants.GROUP;
import static com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoMapper.TENANT;

/**
 * ExternalConfigInfoTagPersistServiceImpl.
 *
 * @author lixiaoshuang
 */
@SuppressWarnings(value = {"PMD.MethodReturnWrapperTypeRule", "checkstyle:linelength"})
@Conditional(value = ConditionOnExternalStorage.class)
@Service
public class MybatisFlexConfigInfoTagPersistServiceImpl implements ConfigInfoTagPersistService {

    @Autowired
    private ConfigInfoTagEntityMapper configInfoTagEntityMapper;

    @Autowired
    protected TransactionTemplate tjt;

    @Override
    @Deprecated
    public <E> PaginationHelper<E> createPaginationHelper() {
        return null;
    }

    @Override
    public void addConfigInfo4Tag(ConfigInfo configInfo, String tag, String srcIp, String srcUser, Timestamp time,
                                  boolean notify) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
        String tagTmp = StringUtils.isBlank(tag) ? StringUtils.EMPTY : tag.trim();
        String md5 = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
        try {
            ConfigInfoTagEntity configInfoTagEntity = new ConfigInfoTagEntity();
            configInfoTagEntity.setDataId(configInfo.getDataId());
            configInfoTagEntity.setGroupId(configInfo.getGroup());
            configInfoTagEntity.setTenantId(tenantTmp);
            configInfoTagEntity.setTagId(tagTmp);
            configInfoTagEntity.setAppName(appNameTmp);
            configInfoTagEntity.setContent(configInfo.getContent());
            configInfoTagEntity.setMd5(md5);
            configInfoTagEntity.setSrcIp(srcIp);
            configInfoTagEntity.setSrcUser(srcUser);
            configInfoTagEntity.setGmtCreate(time.toLocalDateTime());
            configInfoTagEntity.setGmtModified(time.toLocalDateTime());
            configInfoTagEntityMapper.insert(configInfoTagEntity);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public void insertOrUpdateTag(final ConfigInfo configInfo, final String tag, final String srcIp,
                                  final String srcUser, final Timestamp time, final boolean notify) {
        try {
            addConfigInfo4Tag(configInfo, tag, srcIp, null, time, notify);
        } catch (DataIntegrityViolationException ive) { // Unique constraint conflict
            updateConfigInfo4Tag(configInfo, tag, srcIp, null, time, notify);
        }
    }

    @Override
    public boolean insertOrUpdateTagCas(final ConfigInfo configInfo, final String tag, final String srcIp,
                                        final String srcUser, final Timestamp time, final boolean notify) {
        try {
            addConfigInfo4Tag(configInfo, tag, srcIp, null, time, notify);
            return true;
        } catch (DataIntegrityViolationException ive) { // Unique constraint conflict
            return updateConfigInfo4TagCas(configInfo, tag, srcIp, null, time, notify);
        }
    }

    @Override
    public void removeConfigInfoTag(final String dataId, final String group, final String tenant, final String tag,
                                    final String srcIp, final String srcUser) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        String tagTmp = StringUtils.isBlank(tag) ? StringUtils.EMPTY : tag;
        try {
            QueryWrapper queryWrapper = QueryWrapper.create();
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getDataId, dataId);
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getGroupId, group);
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getTenantId, tenantTmp);
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getTagId, tagTmp);
            configInfoTagEntityMapper.deleteByQuery(queryWrapper);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public void updateConfigInfo4Tag(ConfigInfo configInfo, String tag, String srcIp, String srcUser, Timestamp time,
                                     boolean notify) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
        String tagTmp = StringUtils.isBlank(tag) ? StringUtils.EMPTY : tag.trim();
        try {
            String md5 = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
            ConfigInfoTagEntity configInfoTagEntity = new ConfigInfoTagEntity();
            configInfoTagEntity.setContent(configInfo.getContent());
            configInfoTagEntity.setMd5(md5);
            configInfoTagEntity.setSrcIp(srcIp);
            configInfoTagEntity.setSrcUser(srcUser);
            configInfoTagEntity.setGmtModified(time.toLocalDateTime());
            configInfoTagEntity.setAppName(appNameTmp);

            QueryWrapper queryWrapper = QueryWrapper.create();
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getDataId, configInfo.getDataId());
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getGroupId, configInfo.getGroup());
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getTenantId, tenantTmp);
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getTagId, tagTmp);
            configInfoTagEntityMapper.updateByQuery(configInfoTagEntity, queryWrapper);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public boolean updateConfigInfo4TagCas(ConfigInfo configInfo, String tag, String srcIp, String srcUser,
                                           Timestamp time, boolean notify) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
        String tagTmp = StringUtils.isBlank(tag) ? StringUtils.EMPTY : tag.trim();
        try {
            String md5 = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
            ConfigInfoTagEntity configInfoTagEntity = new ConfigInfoTagEntity();
            configInfoTagEntity.setContent(configInfo.getContent());
            configInfoTagEntity.setMd5(md5);
            configInfoTagEntity.setSrcIp(srcIp);
            configInfoTagEntity.setSrcUser(srcUser);
            configInfoTagEntity.setGmtModified(time.toLocalDateTime());
            configInfoTagEntity.setAppName(appNameTmp);

            QueryWrapper queryWrapper = QueryWrapper.create();
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getDataId, configInfo.getDataId());
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getGroupId, configInfo.getGroup());
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getTenantId, tenantTmp);
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getTagId, tagTmp);
            QueryCondition queryCondition = QueryCondition.createEmpty()
                    .or(ConfigInfoTagEntityTableDef.CONFIG_INFO_TAG_ENTITY.MD5.eq(configInfo.getMd5()))
                    .or(ConfigInfoTagEntityTableDef.CONFIG_INFO_TAG_ENTITY.MD5.isNull())
                    .or(ConfigInfoTagEntityTableDef.CONFIG_INFO_TAG_ENTITY.MD5.eq(""));
            queryWrapper.and(queryCondition);
            return configInfoTagEntityMapper.updateByQuery(configInfoTagEntity, queryWrapper) > 0;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public ConfigInfoTagWrapper findConfigInfo4Tag(final String dataId, final String group, final String tenant,
                                                   final String tag) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        String tagTmp = StringUtils.isBlank(tag) ? StringUtils.EMPTY : tag.trim();
        try {
            QueryWrapper queryWrapper = QueryWrapper.create();
            queryWrapper.select(ConfigInfoTagEntity::getId);
            queryWrapper.select(ConfigInfoTagEntity::getDataId);
            queryWrapper.select(ConfigInfoTagEntity::getGroupId).as(GROUP);
            queryWrapper.select(ConfigInfoTagEntity::getTenantId).as(TENANT);
            queryWrapper.select(ConfigInfoTagEntity::getTagId).as("tag");
            queryWrapper.select(ConfigInfoTagEntity::getAppName);
            queryWrapper.select(ConfigInfoTagEntity::getContent);
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getDataId, dataId);
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getGroupId, group);
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getTenantId, tenantTmp);
            MybatisFlexUtils.concatEq(queryWrapper, ConfigInfoTagEntity::getTagId, tagTmp);
            return configInfoTagEntityMapper.selectOneByQueryAs(queryWrapper, ConfigInfoTagWrapper.class);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null.
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public int configInfoTagCount() {
        return (int) configInfoTagEntityMapper.selectCountByQuery(QueryWrapper.create());
    }

    @Override
    public Page<ConfigInfoTagWrapper> findAllConfigInfoTagForDumpAll(final int pageNo, final int pageSize) {
        QueryWrapper queryWrapper = QueryWrapper.create();
        queryWrapper.select(ConfigInfoTagEntity::getId);
        queryWrapper.select(ConfigInfoTagEntity::getDataId);
        queryWrapper.select(ConfigInfoTagEntity::getGroupId).as(GROUP);
        queryWrapper.select(ConfigInfoTagEntity::getTenantId).as(TENANT);
        queryWrapper.select(ConfigInfoTagEntity::getTagId).as("tag");
        queryWrapper.select(ConfigInfoTagEntity::getAppName);
        queryWrapper.select(ConfigInfoTagEntity::getContent);
        queryWrapper.select(ConfigInfoTagEntity::getMd5);
        queryWrapper.select(ConfigInfoTagEntity::getGmtModified);
        try {
            return MybatisFlexUtils.convertPage(configInfoTagEntityMapper.paginateAs(pageNo, pageSize, queryWrapper, ConfigInfoTagWrapper.class));
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

}
