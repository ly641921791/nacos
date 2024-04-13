package com.alibaba.nacos.config.server.service.repository.mybatisflex;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.NamespaceUtil;
import com.alibaba.nacos.common.utils.Pair;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.configuration.ConditionOnExternalStorage;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.enums.FileTypeEnum;
import com.alibaba.nacos.config.server.model.*;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoPersistService;
import com.alibaba.nacos.config.server.service.repository.HistoryConfigInfoPersistService;
import com.alibaba.nacos.config.server.service.repository.PaginationHelper;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.ConfigInfoEntity;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.ConfigTagsRelationEntity;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.table.ConfigInfoBetaEntityTableDef;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.mapper.ConfigInfoEntityMapper;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.mapper.ConfigTagsRelationEntityMapper;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.ParamUtils;
import com.alibaba.nacos.plugin.encryption.handler.EncryptionHandler;
import com.mybatisflex.core.query.QueryCondition;
import com.mybatisflex.core.query.QueryMethods;
import com.mybatisflex.core.query.QueryWrapper;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.dao.*;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

import static com.alibaba.nacos.api.common.Constants.GROUP;
import static com.alibaba.nacos.config.server.service.repository.mybatisflex.MybatisFlexUtils.*;
import static com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.table.ConfigInfoEntityTableDef.CONFIG_INFO_ENTITY;
import static com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.table.ConfigTagsRelationEntityTableDef.CONFIG_TAGS_RELATION_ENTITY;
import static com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoMapper.TENANT;

/**
 * ExternalConfigInfoPersistServiceImpl.
 *
 * @author lixiaoshuang
 */
@SuppressWarnings(value = {"PMD.MethodReturnWrapperTypeRule", "checkstyle:linelength"})
@Conditional(value = ConditionOnExternalStorage.class)
@Service
public class MybatisFlexConfigInfoPersistServiceImpl implements ConfigInfoPersistService {

    /**
     * constant variables.
     */
    public static final String SPOT = ".";

    @Autowired
    protected TransactionTemplate tjt;

    @Autowired
    private ConfigInfoEntityMapper configInfoEntityMapper;

    @Autowired
    private ConfigTagsRelationEntityMapper configTagsRelationEntityMapper;

    @Autowired
    private HistoryConfigInfoPersistService historyConfigInfoPersistService;

    @Override
    @Deprecated
    public <E> PaginationHelper<E> createPaginationHelper() {
        return null;
    }

    @Override
    public String generateLikeArgument(String s) {
        String fuzzySearchSign = "\\*";
        String sqlLikePercentSign = "%";
        if (s.contains(PATTERN_STR)) {
            return s.replaceAll(fuzzySearchSign, sqlLikePercentSign);
        } else {
            return s;
        }
    }

    @Override
    public void addConfigInfo(final String srcIp, final String srcUser, final ConfigInfo configInfo, final Timestamp time, final Map<String, Object> configAdvanceInfo, final boolean notify) {
        tjt.execute(status -> {
            try {
                long configId = addConfigInfoAtomic(-1, srcIp, srcUser, configInfo, time, configAdvanceInfo);
                String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
                addConfigTagsRelation(configId, configTags, configInfo.getDataId(), configInfo.getGroup(), configInfo.getTenant());

                historyConfigInfoPersistService.insertConfigHistoryAtomic(0, configInfo, srcIp, srcUser, time, "I");
            } catch (CannotGetJdbcConnectionException e) {
                LogUtil.FATAL_LOG.error("[db-error] " + e, e);
                throw e;
            }
            return Boolean.TRUE;
        });
    }

    @Override
    public void insertOrUpdate(String srcIp, String srcUser, ConfigInfo configInfo, Timestamp time, Map<String, Object> configAdvanceInfo) {
        insertOrUpdate(srcIp, srcUser, configInfo, time, configAdvanceInfo, true);
    }

    @Override
    public void insertOrUpdate(String srcIp, String srcUser, ConfigInfo configInfo, Timestamp time, Map<String, Object> configAdvanceInfo, boolean notify) {
        try {
            addConfigInfo(srcIp, srcUser, configInfo, time, configAdvanceInfo, notify);
        } catch (DuplicateKeyException ive) { // Unique constraint conflict
            updateConfigInfo(configInfo, srcIp, srcUser, time, configAdvanceInfo, notify);
        }
    }

    @Override
    public boolean insertOrUpdateCas(String srcIp, String srcUser, ConfigInfo configInfo, Timestamp time, Map<String, Object> configAdvanceInfo) {
        return insertOrUpdateCas(srcIp, srcUser, configInfo, time, configAdvanceInfo, true);
    }

    @Override
    public boolean insertOrUpdateCas(String srcIp, String srcUser, ConfigInfo configInfo, Timestamp time, Map<String, Object> configAdvanceInfo, boolean notify) {
        try {
            addConfigInfo(srcIp, srcUser, configInfo, time, configAdvanceInfo, notify);
            return true;
        } catch (DuplicateKeyException ignore) { // Unique constraint conflict
            return updateConfigInfoCas(configInfo, srcIp, srcUser, time, configAdvanceInfo, notify);
        }
    }

    @Override
    public long addConfigInfoAtomic(final long configId, final String srcIp, final String srcUser, final ConfigInfo configInfo, final Timestamp time, Map<String, Object> configAdvanceInfo) {
        final String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        final String tenantTmp = StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();

        final String desc = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("desc");
        final String use = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("use");
        final String effect = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("effect");
        final String type = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("type");
        final String schema = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("schema");
        final String encryptedDataKey = configInfo.getEncryptedDataKey() == null ? StringUtils.EMPTY : configInfo.getEncryptedDataKey();

        final String md5Tmp = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);

        try {
            ConfigInfoEntity configInfoEntity = new ConfigInfoEntity();
            configInfoEntity.setDataId(configInfo.getDataId());
            configInfoEntity.setGroupId(configInfo.getGroup());
            configInfoEntity.setTenantId(tenantTmp);
            configInfoEntity.setAppName(appNameTmp);
            configInfoEntity.setContent(configInfo.getContent());
            configInfoEntity.setMd5(md5Tmp);
            configInfoEntity.setSrcIp(srcIp);
            configInfoEntity.setSrcUser(srcUser);
            configInfoEntity.setGmtCreate(time.toLocalDateTime());
            configInfoEntity.setGmtModified(time.toLocalDateTime());
            configInfoEntity.setCDesc(desc);
            configInfoEntity.setCUse(use);
            configInfoEntity.setEffect(effect);
            configInfoEntity.setType(type);
            configInfoEntity.setCSchema(schema);
            configInfoEntity.setEncryptedDataKey(encryptedDataKey);
            configInfoEntityMapper.insert(configInfoEntity);
            return configInfoEntity.getId();
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }


    @Override
    public void addConfigTagRelationAtomic(long configId, String tagName, String dataId, String group, String tenant) {
        try {
            ConfigTagsRelationEntity configTagsRelationEntity = new ConfigTagsRelationEntity();
            configTagsRelationEntity.setId(configId);
            configTagsRelationEntity.setTagName(tagName);
            configTagsRelationEntity.setTagType(StringUtils.EMPTY);
            configTagsRelationEntity.setDataId(dataId);
            configTagsRelationEntity.setGroupId(group);
            configTagsRelationEntity.setTenantId(tenant);
            configTagsRelationEntityMapper.insert(configTagsRelationEntity);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public void addConfigTagsRelation(long configId, String configTags, String dataId, String group, String tenant) {
        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            for (int i = 0; i < tagArr.length; i++) {
                addConfigTagRelationAtomic(configId, tagArr[i], dataId, group, tenant);
            }
        }
    }

    @Override
    public Map<String, Object> batchInsertOrUpdate(List<ConfigAllInfo> configInfoList, String srcUser, String srcIp, Map<String, Object> configAdvanceInfo, Timestamp time, boolean notify, SameConfigPolicy policy) throws NacosException {
        int succCount = 0;
        int skipCount = 0;
        List<Map<String, String>> failData = null;
        List<Map<String, String>> skipData = null;

        for (int i = 0; i < configInfoList.size(); i++) {
            ConfigAllInfo configInfo = configInfoList.get(i);
            try {
                ParamUtils.checkParam(configInfo.getDataId(), configInfo.getGroup(), "datumId", configInfo.getContent());
            } catch (NacosException e) {
                LogUtil.DEFAULT_LOG.error("data verification failed", e);
                throw e;
            }
            ConfigInfo configInfo2Save = new ConfigInfo(configInfo.getDataId(), configInfo.getGroup(), configInfo.getTenant(), configInfo.getAppName(), configInfo.getContent());
            configInfo2Save.setEncryptedDataKey(configInfo.getEncryptedDataKey() == null ? StringUtils.EMPTY : configInfo.getEncryptedDataKey());

            String type = configInfo.getType();
            if (StringUtils.isBlank(type)) {
                // simple judgment of file type based on suffix
                if (configInfo.getDataId().contains(SPOT)) {
                    String extName = configInfo.getDataId().substring(configInfo.getDataId().lastIndexOf(SPOT) + 1);
                    FileTypeEnum fileTypeEnum = FileTypeEnum.getFileTypeEnumByFileExtensionOrFileType(extName);
                    type = fileTypeEnum.getFileType();
                } else {
                    type = FileTypeEnum.getFileTypeEnumByFileExtensionOrFileType(null).getFileType();
                }
            }
            if (configAdvanceInfo == null) {
                configAdvanceInfo = new HashMap<>(16);
            }
            configAdvanceInfo.put("type", type);
            configAdvanceInfo.put("desc", configInfo.getDesc());
            try {
                addConfigInfo(srcIp, srcUser, configInfo2Save, time, configAdvanceInfo, notify);
                succCount++;
            } catch (DataIntegrityViolationException ive) {
                // uniqueness constraint conflict
                if (SameConfigPolicy.ABORT.equals(policy)) {
                    failData = new ArrayList<>();
                    skipData = new ArrayList<>();
                    Map<String, String> faileditem = new HashMap<>(2);
                    faileditem.put("dataId", configInfo2Save.getDataId());
                    faileditem.put("group", configInfo2Save.getGroup());
                    failData.add(faileditem);
                    for (int j = (i + 1); j < configInfoList.size(); j++) {
                        ConfigInfo skipConfigInfo = configInfoList.get(j);
                        Map<String, String> skipitem = new HashMap<>(2);
                        skipitem.put("dataId", skipConfigInfo.getDataId());
                        skipitem.put("group", skipConfigInfo.getGroup());
                        skipData.add(skipitem);
                    }
                    break;
                } else if (SameConfigPolicy.SKIP.equals(policy)) {
                    skipCount++;
                    if (skipData == null) {
                        skipData = new ArrayList<>();
                    }
                    Map<String, String> skipitem = new HashMap<>(2);
                    skipitem.put("dataId", configInfo2Save.getDataId());
                    skipitem.put("group", configInfo2Save.getGroup());
                    skipData.add(skipitem);
                } else if (SameConfigPolicy.OVERWRITE.equals(policy)) {
                    succCount++;
                    updateConfigInfo(configInfo2Save, srcIp, srcUser, time, configAdvanceInfo, notify);
                }
            }
        }
        Map<String, Object> result = new HashMap<>(4);
        result.put("succCount", succCount);
        result.put("skipCount", skipCount);
        if (failData != null && !failData.isEmpty()) {
            result.put("failData", failData);
        }
        if (skipData != null && !skipData.isEmpty()) {
            result.put("skipData", skipData);
        }
        return result;
    }

    @Override
    public void removeConfigInfo(final String dataId, final String group, final String tenant, final String srcIp, final String srcUser) {
        tjt.execute(new TransactionCallback<Boolean>() {
            final Timestamp time = new Timestamp(System.currentTimeMillis());

            @Override
            public Boolean doInTransaction(TransactionStatus status) {
                try {
                    ConfigInfo configInfo = findConfigInfo(dataId, group, tenant);
                    if (configInfo != null) {
                        removeConfigInfoAtomic(dataId, group, tenant, srcIp, srcUser);
                        removeTagByIdAtomic(configInfo.getId());
                        historyConfigInfoPersistService.insertConfigHistoryAtomic(configInfo.getId(), configInfo, srcIp, srcUser, time, "D");
                    }
                } catch (CannotGetJdbcConnectionException e) {
                    LogUtil.FATAL_LOG.error("[db-error] " + e, e);
                    throw e;
                }
                return Boolean.TRUE;
            }
        });
    }

    @Override
    public List<ConfigInfo> removeConfigInfoByIds(final List<Long> ids, final String srcIp, final String srcUser) {
        if (CollectionUtils.isEmpty(ids)) {
            return null;
        }
        ids.removeAll(Collections.singleton(null));
        return tjt.execute(new TransactionCallback<List<ConfigInfo>>() {
            final Timestamp time = new Timestamp(System.currentTimeMillis());

            @Override
            public List<ConfigInfo> doInTransaction(TransactionStatus status) {
                try {
                    String idsStr = StringUtils.join(ids, StringUtils.COMMA);
                    List<ConfigInfo> configInfoList = findConfigInfosByIds(idsStr);
                    if (!CollectionUtils.isEmpty(configInfoList)) {
                        removeConfigInfoByIdsAtomic(idsStr);
                        for (ConfigInfo configInfo : configInfoList) {
                            removeTagByIdAtomic(configInfo.getId());
                            historyConfigInfoPersistService.insertConfigHistoryAtomic(configInfo.getId(), configInfo, srcIp, srcUser, time, "D");
                        }
                    }
                    return configInfoList;
                } catch (CannotGetJdbcConnectionException e) {
                    LogUtil.FATAL_LOG.error("[db-error] " + e, e);
                    throw e;
                }
            }
        });
    }

    @Override
    public void removeTagByIdAtomic(long id) {
        try {
            configTagsRelationEntityMapper.deleteById(id);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public void removeConfigInfoAtomic(final String dataId, final String group, final String tenant, final String srcIp, final String srcUser) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        try {
            QueryWrapper queryWrapper = QueryWrapper.create();
            concatEq(queryWrapper, ConfigInfoEntity::getDataId, dataId);
            concatEq(queryWrapper, ConfigInfoEntity::getGroupId, group);
            concatEq(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
            configInfoEntityMapper.deleteByQuery(queryWrapper);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public void removeConfigInfoByIdsAtomic(final String ids) {
        if (StringUtils.isBlank(ids)) {
            return;
        }
        List<Long> paramList = new ArrayList<>();
        String[] tagArr = ids.split(",");
        for (String s : tagArr) {
            paramList.add(Long.parseLong(s));
        }

        try {
            configInfoEntityMapper.deleteBatchByIds(paramList);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public void updateConfigInfo(final ConfigInfo configInfo, final String srcIp, final String srcUser, final Timestamp time, final Map<String, Object> configAdvanceInfo, final boolean notify) {
        tjt.execute(status -> {
            try {
                ConfigInfo oldConfigInfo = findConfigInfo(configInfo.getDataId(), configInfo.getGroup(), configInfo.getTenant());
                if (oldConfigInfo == null) {
                    if (LogUtil.FATAL_LOG.isErrorEnabled()) {
                        LogUtil.FATAL_LOG.error("expected config info[dataid:{}, group:{}, tenent:{}] but not found.", configInfo.getDataId(), configInfo.getGroup(), configInfo.getTenant());
                    }
                    return Boolean.FALSE;
                }

                String appNameTmp = oldConfigInfo.getAppName();
                /*
                 If the appName passed by the user is not empty, use the persistent user's appName,
                 otherwise use db; when emptying appName, you need to pass an empty string
                 */
                if (configInfo.getAppName() == null) {
                    configInfo.setAppName(appNameTmp);
                }
                updateConfigInfoAtomic(configInfo, srcIp, srcUser, time, configAdvanceInfo);
                String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
                if (configTags != null) {
                    // delete all tags and then recreate
                    removeTagByIdAtomic(oldConfigInfo.getId());
                    addConfigTagsRelation(oldConfigInfo.getId(), configTags, configInfo.getDataId(), configInfo.getGroup(), configInfo.getTenant());
                }
                historyConfigInfoPersistService.insertConfigHistoryAtomic(oldConfigInfo.getId(), oldConfigInfo, srcIp, srcUser, time, "U");
            } catch (CannotGetJdbcConnectionException e) {
                LogUtil.FATAL_LOG.error("[db-error] " + e, e);
                throw e;
            }
            return Boolean.TRUE;
        });
    }

    @Override
    public boolean updateConfigInfoCas(final ConfigInfo configInfo, final String srcIp, final String srcUser, final Timestamp time, final Map<String, Object> configAdvanceInfo, final boolean notify) {
        return tjt.execute(status -> {
            try {
                ConfigInfo oldConfigInfo = findConfigInfo(configInfo.getDataId(), configInfo.getGroup(), configInfo.getTenant());
                if (oldConfigInfo == null) {
                    if (LogUtil.FATAL_LOG.isErrorEnabled()) {
                        LogUtil.FATAL_LOG.error("expected config info[dataid:{}, group:{}, tenent:{}] but not found.", configInfo.getDataId(), configInfo.getGroup(), configInfo.getTenant());
                    }
                    return Boolean.FALSE;
                }
                String appNameTmp = oldConfigInfo.getAppName();
                /*
                 If the appName passed by the user is not empty, use the persistent user's appName,
                 otherwise use db; when emptying appName, you need to pass an empty string
                 */
                if (configInfo.getAppName() == null) {
                    configInfo.setAppName(appNameTmp);
                }
                int rows = updateConfigInfoAtomicCas(configInfo, srcIp, srcUser, time, configAdvanceInfo);
                if (rows < 1) {
                    return Boolean.FALSE;
                }
                String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
                if (configTags != null) {
                    // delete all tags and then recreate
                    removeTagByIdAtomic(oldConfigInfo.getId());
                    addConfigTagsRelation(oldConfigInfo.getId(), configTags, configInfo.getDataId(), configInfo.getGroup(), configInfo.getTenant());
                }
                historyConfigInfoPersistService.insertConfigHistoryAtomic(oldConfigInfo.getId(), oldConfigInfo, srcIp, srcUser, time, "U");
            } catch (CannotGetJdbcConnectionException e) {
                LogUtil.FATAL_LOG.error("[db-error] " + e, e);
                throw e;
            }
            return Boolean.TRUE;
        });
    }

    private int updateConfigInfoAtomicCas(final ConfigInfo configInfo, final String srcIp, final String srcUser, final Timestamp time, Map<String, Object> configAdvanceInfo) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
        final String md5Tmp = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
        String desc = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("desc");
        String use = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("use");
        String effect = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("effect");
        String type = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("type");
        String schema = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("schema");

        ConfigInfoEntity configInfoEntity = new ConfigInfoEntity();
        configInfoEntity.setContent(configInfo.getContent());
        configInfoEntity.setMd5(md5Tmp);
        configInfoEntity.setSrcIp(srcIp);
        configInfoEntity.setSrcUser(srcUser);
        configInfoEntity.setGmtModified(time.toLocalDateTime());
        configInfoEntity.setAppName(appNameTmp);
        configInfoEntity.setCDesc(desc);
        configInfoEntity.setCUse(use);
        configInfoEntity.setEffect(effect);
        configInfoEntity.setType(type);
        configInfoEntity.setCSchema(schema);

        QueryWrapper queryWrapper = QueryWrapper.create();
        concatEq(queryWrapper, ConfigInfoEntity::getDataId, configInfo.getDataId());
        concatEq(queryWrapper, ConfigInfoEntity::getGroupId, configInfo.getGroup());
        concatEq(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
        QueryCondition queryCondition = QueryCondition.createEmpty().or(ConfigInfoBetaEntityTableDef.CONFIG_INFO_BETA_ENTITY.MD5.eq(configInfo.getMd5())).or(ConfigInfoBetaEntityTableDef.CONFIG_INFO_BETA_ENTITY.MD5.isNull()).or(ConfigInfoBetaEntityTableDef.CONFIG_INFO_BETA_ENTITY.MD5.eq(StringUtils.EMPTY));
        queryWrapper.and(queryCondition);

        try {
            return configInfoEntityMapper.updateByQuery(configInfoEntity, queryWrapper);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public void updateConfigInfoAtomic(final ConfigInfo configInfo, final String srcIp, final String srcUser, final Timestamp time, Map<String, Object> configAdvanceInfo) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
        final String md5Tmp = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
        String desc = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("desc");
        String use = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("use");
        String effect = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("effect");
        String type = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("type");
        String schema = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("schema");
        final String encryptedDataKey = configInfo.getEncryptedDataKey() == null ? StringUtils.EMPTY : configInfo.getEncryptedDataKey();

        ConfigInfoEntity configInfoEntity = new ConfigInfoEntity();
        configInfoEntity.setContent(configInfo.getContent());
        configInfoEntity.setMd5(md5Tmp);
        configInfoEntity.setSrcIp(srcIp);
        configInfoEntity.setSrcUser(srcUser);
        configInfoEntity.setGmtModified(time.toLocalDateTime());
        configInfoEntity.setAppName(appNameTmp);
        configInfoEntity.setCDesc(desc);
        configInfoEntity.setCUse(use);
        configInfoEntity.setEffect(effect);
        configInfoEntity.setType(type);
        configInfoEntity.setCSchema(schema);
        configInfoEntity.setEncryptedDataKey(encryptedDataKey);

        QueryWrapper queryWrapper = QueryWrapper.create();
        concatEq(queryWrapper, ConfigInfoEntity::getDataId, configInfo.getDataId());
        concatEq(queryWrapper, ConfigInfoEntity::getGroupId, configInfo.getGroup());
        concatEq(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
        try {
            configInfoEntityMapper.updateByQuery(configInfoEntity, queryWrapper);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    @Deprecated
    public void updateMd5(String dataId, String group, String tenant, String md5, Timestamp lastTime) {
    }

    @Override
    public long findConfigMaxId() {
        QueryWrapper queryWrapper = QueryWrapper.create();
        queryWrapper.select(QueryMethods.max(CONFIG_INFO_ENTITY.ID));
        try {
            return configInfoEntityMapper.selectOneByQueryAs(queryWrapper, Long.class);
        } catch (NullPointerException e) {
            return 0;
        }
    }

    @Override
    @Deprecated
    public List<ConfigInfo> findAllDataIdAndGroup() {
        return null;
    }

    @Override
    public ConfigInfoBase findConfigInfoBase(final String dataId, final String group) {
        try {
            QueryWrapper queryWrapper = QueryWrapper.create()
                    .select(ConfigInfoEntity::getId)
                    .select(ConfigInfoEntity::getDataId)
                    .select(ConfigInfoEntity::getGroupId).as(GROUP)
                    .select(ConfigInfoEntity::getContent);
            concatEq(queryWrapper, ConfigInfoEntity::getDataId, dataId);
            concatEq(queryWrapper, ConfigInfoEntity::getGroupId, group);
            concatEq(queryWrapper, ConfigInfoEntity::getTenantId, StringUtils.EMPTY);
            return configInfoEntityMapper.selectOneByQueryAs(queryWrapper, ConfigInfoBase.class);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null.
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public ConfigInfo findConfigInfo(long id) {
        try {
            QueryWrapper queryWrapper = QueryWrapper.create()
                    .select(ConfigInfoEntity::getId)
                    .select(ConfigInfoEntity::getDataId)
                    .select(ConfigInfoEntity::getGroupId).as(GROUP)
                    .select(ConfigInfoEntity::getTenantId).as(TENANT)
                    .select(ConfigInfoEntity::getAppName)
                    .select(ConfigInfoEntity::getContent)
                    .eq(ConfigInfoEntity::getId, id);
            return configInfoEntityMapper.selectOneByQueryAs(queryWrapper, ConfigInfo.class);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null.
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public ConfigInfoWrapper findConfigInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        try {
            QueryWrapper queryWrapper = QueryWrapper.create()
                    .select(ConfigInfoEntity::getId)
                    .select(ConfigInfoEntity::getDataId)
                    .select(ConfigInfoEntity::getGroupId).as(GROUP)
                    .select(ConfigInfoEntity::getTenantId).as(TENANT)
                    .select(ConfigInfoEntity::getAppName)
                    .select(ConfigInfoEntity::getContent)
                    .select(ConfigInfoEntity::getMd5)
                    .select(ConfigInfoEntity::getType)
                    .select(ConfigInfoEntity::getEncryptedDataKey);
            concatEq(queryWrapper, ConfigInfoEntity::getDataId, dataId);
            concatEq(queryWrapper, ConfigInfoEntity::getGroupId, group);
            concatEq(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
            return configInfoEntityMapper.selectOneByQueryAs(queryWrapper, ConfigInfoWrapper.class);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null.
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public Page<ConfigInfo> findConfigInfo4Page(final int pageNo, final int pageSize, final String dataId, final String group, final String tenant, final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        final String appName = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("appName");
        final String content = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("content");
        final String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
        QueryWrapper queryWrapper = QueryWrapper.create()
                .select(ConfigInfoEntity::getId)
                .select(ConfigInfoEntity::getDataId)
                .select(ConfigInfoEntity::getGroupId).as(GROUP)
                .select(ConfigInfoEntity::getTenantId).as(TENANT)
                .select(ConfigInfoEntity::getAppName)
                .select(ConfigInfoEntity::getContent)
                .select(ConfigInfoEntity::getType)
                .select(ConfigInfoEntity::getEncryptedDataKey);
        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            queryWrapper
                    .from(CONFIG_INFO_ENTITY.as("a"))
                    .leftJoin(CONFIG_TAGS_RELATION_ENTITY.as("b")).on(CONFIG_INFO_ENTITY.ID.eq(CONFIG_TAGS_RELATION_ENTITY.ID));
            concatEq(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
            if (StringUtils.isNotBlank(dataId)) {
                queryWrapper.eq(ConfigInfoEntity::getDataId, dataId);
            }
            if (StringUtils.isNotBlank(group)) {
                queryWrapper.eq(ConfigInfoEntity::getGroupId, group);
            }
            if (StringUtils.isNotBlank(appName)) {
                queryWrapper.eq(ConfigInfoEntity::getAppName, appName);
            }
            if (StringUtils.isNotBlank(content)) {
                queryWrapper.like(ConfigInfoEntity::getContent, content);
            }
            concatIn(queryWrapper, CONFIG_TAGS_RELATION_ENTITY.TAG_NAME, Arrays.asList(tagArr));
        } else {
            concatEq(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
            if (StringUtils.isNotBlank(dataId)) {
                queryWrapper.eq(ConfigInfoEntity::getDataId, dataId);
            }
            if (StringUtils.isNotBlank(group)) {
                queryWrapper.eq(ConfigInfoEntity::getGroupId, group);
            }
            if (StringUtils.isNotBlank(appName)) {
                queryWrapper.eq(ConfigInfoEntity::getAppName, appName);
            }
            if (StringUtils.isNotBlank(content)) {
                queryWrapper.like(ConfigInfoEntity::getContent, content);
            }
        }
        try {
            Page<ConfigInfo> page = convertPage(configInfoEntityMapper.paginateAs(pageNo, pageSize, queryWrapper, ConfigInfo.class));
            for (ConfigInfo configInfo : page.getPageItems()) {
                Pair<String, String> pair = EncryptionHandler.decryptHandler(configInfo.getDataId(), configInfo.getEncryptedDataKey(), configInfo.getContent());
                configInfo.setContent(pair.getSecond());
            }
            return page;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] ", e);
            throw e;
        }
    }

    @Override
    @Deprecated
    public Page<ConfigInfo> findConfigInfoByApp(final int pageNo, final int pageSize, final String tenant, final String appName) {
        return null;
    }

    @Override
    @Deprecated
    public Page<ConfigInfoBase> findConfigInfoBaseByGroup(final int pageNo, final int pageSize, final String group) {
        return null;
    }

    @Override
    public int configInfoCount() {
        return (int) configInfoEntityMapper.selectCountByQuery(QueryWrapper.create());
    }

    @Override
    public int configInfoCount(String tenant) {
        QueryWrapper queryWrapper = QueryWrapper.create();
        concatLike(queryWrapper, ConfigInfoEntity::getTenantId, tenant);
        return (int) configInfoEntityMapper.selectCountByQuery(queryWrapper);
    }

    @Override
    public List<String> getTenantIdList(int page, int pageSize) {
        QueryWrapper queryWrapper = QueryWrapper.create().select(ConfigInfoEntity::getTenantId);
        concatNotEq(queryWrapper, ConfigInfoEntity::getTenantId, NamespaceUtil.getNamespaceDefaultId());
        queryWrapper.groupBy(ConfigInfoEntity::getTenantId);
        return configInfoEntityMapper.paginateAs(page, pageSize, queryWrapper, String.class).getRecords();
    }

    @Override
    public List<String> getGroupIdList(int page, int pageSize) {
        QueryWrapper queryWrapper = QueryWrapper.create().select(ConfigInfoEntity::getGroupId);
        concatNotEq(queryWrapper, ConfigInfoEntity::getTenantId, NamespaceUtil.getNamespaceDefaultId());
        queryWrapper.groupBy(ConfigInfoEntity::getGroupId);
        return configInfoEntityMapper.paginateAs(page, pageSize, queryWrapper, String.class).getRecords();
    }

    @Override
    @Deprecated
    public Page<ConfigInfo> findAllConfigInfo(final int pageNo, final int pageSize, final String tenant) {
        return null;
    }

    @Override
    @Deprecated
    public Page<ConfigKey> findAllConfigKey(final int pageNo, final int pageSize, final String tenant) {
        return null;
    }

    @Override
    public Page<ConfigInfoWrapper> findAllConfigInfoFragment(final long lastMaxId, final int pageSize) {
        QueryWrapper queryWrapper = QueryWrapper.create()
                .select(ConfigInfoEntity::getId)
                .select(ConfigInfoEntity::getDataId)
                .select(ConfigInfoEntity::getGroupId).as(GROUP)
                .select(ConfigInfoEntity::getTenantId).as(TENANT)
                .select(ConfigInfoEntity::getAppName)
                .select(ConfigInfoEntity::getContent)
                .select(ConfigInfoEntity::getMd5)
                .select(ConfigInfoEntity::getGmtModified)
                .select(ConfigInfoEntity::getType)
                .select(ConfigInfoEntity::getEncryptedDataKey)
                .gt(ConfigInfoEntity::getId, lastMaxId);
        try {
            return convertPage(configInfoEntityMapper.paginateAs(1, pageSize, queryWrapper, ConfigInfoWrapper.class));
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    @Deprecated
    public Page<ConfigInfo> findConfigInfoLike(final int pageNo, final int pageSize, final ConfigKey[] configKeys, final boolean blacklist) {
        return null;
    }

    @Override
    public Page<ConfigInfo> findConfigInfoLike4Page(final int pageNo, final int pageSize, final String dataId, final String group, final String tenant, final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        final String appName = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("appName");
        final String content = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("content");
        final String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");

        QueryWrapper queryWrapper = QueryWrapper.create()
                .select(ConfigInfoEntity::getId)
                .select(ConfigInfoEntity::getDataId)
                .select(ConfigInfoEntity::getGroupId).as(GROUP)
                .select(ConfigInfoEntity::getTenantId).as(TENANT)
                .select(ConfigInfoEntity::getAppName)
                .select(ConfigInfoEntity::getContent)
                .select(ConfigInfoEntity::getEncryptedDataKey);
        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            queryWrapper
                    .from(CONFIG_INFO_ENTITY.as("a"))
                    .leftJoin(CONFIG_TAGS_RELATION_ENTITY.as("b")).on(CONFIG_INFO_ENTITY.ID.eq(CONFIG_TAGS_RELATION_ENTITY.ID));
            concatLike(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
            if (StringUtils.isNotBlank(dataId)) {
                queryWrapper.like(ConfigInfoEntity::getDataId, dataId);
            }
            if (StringUtils.isNotBlank(group)) {
                queryWrapper.like(ConfigInfoEntity::getGroupId, group);
            }
            if (StringUtils.isNotBlank(appName)) {
                queryWrapper.eq(ConfigInfoEntity::getAppName, appName);
            }
            if (StringUtils.isNotBlank(content)) {
                queryWrapper.like(ConfigInfoEntity::getContent, content);
            }
            concatIn(queryWrapper, CONFIG_TAGS_RELATION_ENTITY.TAG_NAME, Arrays.asList(tagArr));
        } else {
            concatLike(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
            if (StringUtils.isNotBlank(dataId)) {
                queryWrapper.like(ConfigInfoEntity::getDataId, dataId);
            }
            if (StringUtils.isNotBlank(group)) {
                queryWrapper.like(ConfigInfoEntity::getGroupId, group);
            }
            if (StringUtils.isNotBlank(appName)) {
                queryWrapper.eq(ConfigInfoEntity::getAppName, appName);
            }
            if (StringUtils.isNotBlank(content)) {
                queryWrapper.like(ConfigInfoEntity::getContent, content);
            }
        }

        try {
            Page<ConfigInfo> page = convertPage(configInfoEntityMapper.paginateAs(pageNo, pageSize, queryWrapper, ConfigInfo.class));

            for (ConfigInfo configInfo : page.getPageItems()) {
                Pair<String, String> pair = EncryptionHandler.decryptHandler(configInfo.getDataId(), configInfo.getEncryptedDataKey(), configInfo.getContent());
                configInfo.setContent(pair.getSecond());
            }
            return page;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    @Deprecated
    public Page<ConfigInfoBase> findConfigInfoBaseLike(final int pageNo, final int pageSize, final String dataId, final String group, final String content) throws IOException {
        return null;
    }

    @Override
    public List<ConfigInfoWrapper> findChangeConfig(final Timestamp startTime, final Timestamp endTime) {
        try {
            QueryWrapper queryWrapper = QueryWrapper.create();
            queryWrapper.select(ConfigInfoEntity::getDataId);
            queryWrapper.select(ConfigInfoEntity::getGroupId).as(GROUP);
            queryWrapper.select(ConfigInfoEntity::getTenantId).as(TENANT);
            queryWrapper.select(ConfigInfoEntity::getAppName);
            queryWrapper.select(ConfigInfoEntity::getContent);
            queryWrapper.select(ConfigInfoEntity::getGmtModified);
            queryWrapper.select(ConfigInfoEntity::getEncryptedDataKey);
            queryWrapper.between(ConfigInfoEntity::getGmtModified, startTime, endTime);
            return configInfoEntityMapper.selectListByQueryAs(queryWrapper, ConfigInfoWrapper.class);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public Page<ConfigInfoWrapper> findChangeConfig(final String dataId, final String group, final String tenant, final String appName, final Timestamp startTime, final Timestamp endTime, final int pageNo, final int pageSize, final long lastMaxId) {
        return null;
    }

    @Override
    public List<String> selectTagByConfig(String dataId, String group, String tenant) {
        QueryWrapper queryWrapper = QueryWrapper.create().select(ConfigTagsRelationEntity::getTagName);
        concatEq(queryWrapper, ConfigTagsRelationEntity::getDataId, dataId);
        concatEq(queryWrapper, ConfigTagsRelationEntity::getGroupId, group);
        concatEq(queryWrapper, ConfigTagsRelationEntity::getTenantId, tenant);
        try {
            return configTagsRelationEntityMapper.selectListByQueryAs(queryWrapper, String.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (IncorrectResultSizeDataAccessException e) {
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public List<ConfigInfo> findConfigInfosByIds(final String ids) {
        if (StringUtils.isBlank(ids)) {
            return null;
        }
        List<Long> paramList = new ArrayList<>();
        String[] tagArr = ids.split(",");
        for (int i = 0; i < tagArr.length; i++) {
            paramList.add(Long.parseLong(tagArr[i]));
        }
        try {
            QueryWrapper queryWrapper = QueryWrapper.create()
                    .select(ConfigInfoEntity::getId)
                    .select(ConfigInfoEntity::getDataId)
                    .select(ConfigInfoEntity::getGroupId).as(GROUP)
                    .select(ConfigInfoEntity::getTenantId).as(TENANT)
                    .select(ConfigInfoEntity::getAppName)
                    .select(ConfigInfoEntity::getContent)
                    .select(ConfigInfoEntity::getMd5)
                    .in(ConfigInfoEntity::getId, paramList);
            return configInfoEntityMapper.selectListByQueryAs(queryWrapper, ConfigInfo.class);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public ConfigAdvanceInfo findConfigAdvanceInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        try {
            List<String> configTagList = this.selectTagByConfig(dataId, group, tenant);

            QueryWrapper queryWrapper = QueryWrapper.create().select(ConfigInfoEntity::getGmtCreate).select(ConfigInfoEntity::getGmtModified).select(ConfigInfoEntity::getSrcUser).select(ConfigInfoEntity::getSrcIp).select(ConfigInfoEntity::getCDesc).select(ConfigInfoEntity::getCUse).select(ConfigInfoEntity::getEffect).select(ConfigInfoEntity::getType).select(ConfigInfoEntity::getCSchema);
            concatEq(queryWrapper, ConfigInfoEntity::getDataId, dataId);
            concatEq(queryWrapper, ConfigInfoEntity::getGroupId, group);
            concatEq(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
            ConfigAdvanceInfo configAdvance = configInfoEntityMapper.selectOneByQueryAs(queryWrapper, ConfigAdvanceInfo.class);

            if (configTagList != null && !configTagList.isEmpty()) {
                StringBuilder configTagsTmp = new StringBuilder();
                for (String configTag : configTagList) {
                    if (configTagsTmp.length() == 0) {
                        configTagsTmp.append(configTag);
                    } else {
                        configTagsTmp.append(',').append(configTag);
                    }
                }
                configAdvance.setConfigTags(configTagsTmp.toString());
            }
            return configAdvance;
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public ConfigAllInfo findConfigAllInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        try {
            List<String> configTagList = this.selectTagByConfig(dataId, group, tenant);

            QueryWrapper queryWrapper = QueryWrapper.create()
                    .select(ConfigInfoEntity::getId)
                    .select(ConfigInfoEntity::getDataId)
                    .select(ConfigInfoEntity::getGroupId).as(GROUP)
                    .select(ConfigInfoEntity::getTenantId).as(TENANT)
                    .select(ConfigInfoEntity::getAppName)
                    .select(ConfigInfoEntity::getContent)
                    .select(ConfigInfoEntity::getMd5)
                    .select(ConfigInfoEntity::getGmtCreate)
                    .select(ConfigInfoEntity::getGmtModified)
                    .select(ConfigInfoEntity::getSrcUser)
                    .select(ConfigInfoEntity::getSrcIp)
                    .select(ConfigInfoEntity::getCDesc)
                    .select(ConfigInfoEntity::getCUse)
                    .select(ConfigInfoEntity::getEffect)
                    .select(ConfigInfoEntity::getType)
                    .select(ConfigInfoEntity::getCSchema)
                    .select(ConfigInfoEntity::getEncryptedDataKey);
            concatEq(queryWrapper, ConfigInfoEntity::getDataId, dataId);
            concatEq(queryWrapper, ConfigInfoEntity::getGroupId, group);
            concatEq(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
            ConfigAllInfo configAdvance = configInfoEntityMapper.selectOneByQueryAs(queryWrapper, ConfigAllInfo.class);

            if (configTagList != null && !configTagList.isEmpty()) {
                StringBuilder configTagsTmp = new StringBuilder();
                for (String configTag : configTagList) {
                    if (configTagsTmp.length() == 0) {
                        configTagsTmp.append(configTag);
                    } else {
                        configTagsTmp.append(',').append(configTag);
                    }
                }
                configAdvance.setConfigTags(configTagsTmp.toString());
            }
            return configAdvance;
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    @Deprecated
    public List<ConfigInfo> convertDeletedConfig(List<Map<String, Object>> list) {
        return null;
    }

    @Override
    public List<ConfigInfoWrapper> convertChangeConfig(List<Map<String, Object>> list) {
        List<ConfigInfoWrapper> configs = new ArrayList<>();
        for (Map<String, Object> map : list) {
            String dataId = (String) map.get("data_id");
            String group = (String) map.get("group_id");
            String tenant = (String) map.get("tenant_id");
            String content = (String) map.get("content");
            long mTime = ((Timestamp) map.get("gmt_modified")).getTime();
            ConfigInfoWrapper config = new ConfigInfoWrapper();
            config.setDataId(dataId);
            config.setGroup(group);
            config.setTenant(tenant);
            config.setContent(content);
            config.setLastModified(mTime);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public List<ConfigInfoWrapper> listAllGroupKeyMd5() {
        final int pageSize = 10000;
        int totalCount = configInfoCount();
        int pageCount = (int) Math.ceil(totalCount * 1.0 / pageSize);
        List<ConfigInfoWrapper> allConfigInfo = new ArrayList<>();
        for (int pageNo = 1; pageNo <= pageCount; pageNo++) {
            List<ConfigInfoWrapper> configInfoList = listGroupKeyMd5ByPage(pageNo, pageSize);
            allConfigInfo.addAll(configInfoList);
        }
        return allConfigInfo;
    }

    @Override
    public List<ConfigInfoWrapper> listGroupKeyMd5ByPage(int pageNo, int pageSize) {
        try {
            QueryWrapper queryWrapper = QueryWrapper.create();
            queryWrapper.select(ConfigInfoEntity::getId);
            queryWrapper.select(ConfigInfoEntity::getDataId);
            queryWrapper.select(ConfigInfoEntity::getGroupId).as(GROUP);
            queryWrapper.select(ConfigInfoEntity::getTenantId).as(TENANT);
            queryWrapper.select(ConfigInfoEntity::getAppName);
            queryWrapper.select(ConfigInfoEntity::getMd5);
            queryWrapper.select(ConfigInfoEntity::getType);
            queryWrapper.select(ConfigInfoEntity::getGmtModified);
            queryWrapper.select(ConfigInfoEntity::getEncryptedDataKey);
            return configInfoEntityMapper.paginateAs(pageNo, pageSize, queryWrapper, ConfigInfoWrapper.class).getRecords();
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public ConfigInfoWrapper queryConfigInfo(final String dataId, final String group, final String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        try {
            QueryWrapper queryWrapper = QueryWrapper.create();
            queryWrapper.select(ConfigInfoEntity::getId);
            queryWrapper.select(ConfigInfoEntity::getDataId);
            queryWrapper.select(ConfigInfoEntity::getGroupId).as(GROUP);
            queryWrapper.select(ConfigInfoEntity::getTenantId).as(TENANT);
            queryWrapper.select(ConfigInfoEntity::getAppName);
            queryWrapper.select(ConfigInfoEntity::getContent);
            queryWrapper.select(ConfigInfoEntity::getType);
            queryWrapper.select(ConfigInfoEntity::getGmtModified);
            queryWrapper.select(ConfigInfoEntity::getMd5);
            queryWrapper.select(ConfigInfoEntity::getEncryptedDataKey);
            concatEq(queryWrapper, ConfigInfoEntity::getDataId, dataId);
            concatEq(queryWrapper, ConfigInfoEntity::getGroupId, group);
            concatEq(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
            return configInfoEntityMapper.selectOneByQueryAs(queryWrapper, ConfigInfoWrapper.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public List<ConfigAllInfo> findAllConfigInfo4Export(final String dataId, final String group, final String tenant, final String appName, final List<Long> ids) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        QueryWrapper queryWrapper = QueryWrapper.create()
                .select(ConfigInfoEntity::getId)
                .select(ConfigInfoEntity::getDataId)
                .select(ConfigInfoEntity::getGroupId).as(GROUP)
                .select(ConfigInfoEntity::getTenantId).as(TENANT)
                .select(ConfigInfoEntity::getAppName)
                .select(ConfigInfoEntity::getContent)
                .select(ConfigInfoEntity::getType)
                .select(ConfigInfoEntity::getMd5)
                .select(ConfigInfoEntity::getGmtCreate)
                .select(ConfigInfoEntity::getGmtModified)
                .select(ConfigInfoEntity::getSrcUser)
                .select(ConfigInfoEntity::getSrcIp)
                .select(ConfigInfoEntity::getCDesc)
                .select(ConfigInfoEntity::getCUse)
                .select(ConfigInfoEntity::getEffect)
                .select(ConfigInfoEntity::getCSchema)
                .select(ConfigInfoEntity::getEncryptedDataKey);
        if (CollectionUtils.isNotEmpty(ids)) {
            queryWrapper.in(ConfigInfoEntity::getId, ids);
        } else {
            concatEq(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
            if (StringUtils.isNotBlank(dataId)) {
                queryWrapper.like(ConfigInfoEntity::getDataId, dataId);
            }
            if (StringUtils.isNotBlank(group)) {
                queryWrapper.eq(ConfigInfoEntity::getGroupId, group);
            }
            if (StringUtils.isNotBlank(appName)) {
                queryWrapper.eq(ConfigInfoEntity::getAppName, appName);
            }
        }

        try {
            return configInfoEntityMapper.selectListByQueryAs(queryWrapper, ConfigAllInfo.class);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public List<ConfigInfoWrapper> queryConfigInfoByNamespace(String tenant) {
        if (Objects.isNull(tenant)) {
            throw new IllegalArgumentException("tenantId can not be null");
        }
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        try {
            QueryWrapper queryWrapper = QueryWrapper.create()
                    .select(ConfigInfoEntity::getId)
                    .select(ConfigInfoEntity::getGroupId).as(GROUP)
                    .select(ConfigInfoEntity::getTenantId).as(TENANT)
                    .select(ConfigInfoEntity::getAppName)
                    .select(ConfigInfoEntity::getType);
            concatEq(queryWrapper, ConfigInfoEntity::getTenantId, tenantTmp);
            return configInfoEntityMapper.selectListByQueryAs(queryWrapper, ConfigInfoWrapper.class);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null.
            return Collections.EMPTY_LIST;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    @Deprecated
    public Page<ConfigInfoBase> findAllConfigInfoBase(final int pageNo, final int pageSize) {
        QueryWrapper queryWrapper = QueryWrapper.create()
                .select(ConfigInfoEntity::getId)
                .select(ConfigInfoEntity::getDataId)
                .select(ConfigInfoEntity::getGroupId).as(GROUP)
                .select(ConfigInfoEntity::getContent)
                .select(ConfigInfoEntity::getMd5);
        try {
            return convertPage(configInfoEntityMapper.paginateAs(pageNo, pageSize, queryWrapper, ConfigInfoBase.class));
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }
}
