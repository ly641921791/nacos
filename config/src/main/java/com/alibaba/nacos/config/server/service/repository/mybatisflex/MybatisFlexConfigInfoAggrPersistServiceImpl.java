package com.alibaba.nacos.config.server.service.repository.mybatisflex;

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.configuration.ConditionOnExternalStorage;
import com.alibaba.nacos.config.server.model.ConfigInfoAggr;
import com.alibaba.nacos.config.server.model.ConfigInfoChanged;
import com.alibaba.nacos.config.server.model.ConfigKey;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.repository.ConfigInfoAggrPersistService;
import com.alibaba.nacos.config.server.service.repository.PaginationHelper;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.ConfigInfoAggrEntity;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.table.ConfigInfoAggrEntityTableDef;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.mapper.ConfigInfoAggrEntityMapper;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.mybatisflex.core.query.QueryChain;
import com.mybatisflex.core.query.QueryCondition;
import com.mybatisflex.core.query.QueryMethods;
import com.mybatisflex.core.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alibaba.nacos.config.server.service.repository.mybatisflex.MybatisFlexUtils.*;

/**
 * ExternalConfigInfoAggrPersistServiceImpl.
 *
 * @author lixiaoshuang
 */
@SuppressWarnings(value = {"PMD.MethodReturnWrapperTypeRule", "checkstyle:linelength"})
@Conditional(value = ConditionOnExternalStorage.class)
@Service
public class MybatisFlexConfigInfoAggrPersistServiceImpl implements ConfigInfoAggrPersistService {

    @Autowired
    private ConfigInfoAggrEntityMapper configInfoAggrEntityMapper;

    @Autowired
    protected TransactionTemplate tjt;

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
    public boolean addAggrConfigInfo(final String dataId, final String group, String tenant, final String datumId, String appName, final String content) {
        String appNameTmp = StringUtils.isBlank(appName) ? StringUtils.EMPTY : appName;
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        try {
            try {
                QueryWrapper select = QueryWrapper.create().select(ConfigInfoAggrEntity::getContent);
                concatEq(select, ConfigInfoAggrEntity::getDataId, dataId);
                concatEq(select, ConfigInfoAggrEntity::getGroupId, group);
                concatEq(select, ConfigInfoAggrEntity::getTenantId, tenantTmp);
                concatEq(select, ConfigInfoAggrEntity::getDatumId, datumId);
                String dbContent = configInfoAggrEntityMapper.selectOneByQueryAs(select, String.class);
                if (dbContent != null && dbContent.equals(content)) {
                    return true;
                } else {
                    ConfigInfoAggrEntity configInfoAggrEntity = new ConfigInfoAggrEntity();
                    configInfoAggrEntity.setContent(content);
                    configInfoAggrEntity.setGmtModified(LocalDateTime.now());

                    QueryWrapper update = QueryWrapper.create();
                    concatEq(update, ConfigInfoAggrEntity::getDataId, dataId);
                    concatEq(update, ConfigInfoAggrEntity::getGroupId, group);
                    concatEq(update, ConfigInfoAggrEntity::getTenantId, tenantTmp);
                    concatEq(update, ConfigInfoAggrEntity::getDatumId, datumId);
                    return configInfoAggrEntityMapper.updateByQuery(configInfoAggrEntity, update) > 0;
                }
            } catch (EmptyResultDataAccessException ex) { // no data, insert
                ConfigInfoAggrEntity configInfoAggrEntity = new ConfigInfoAggrEntity();
                configInfoAggrEntity.setDataId(dataId);
                configInfoAggrEntity.setGroupId(group);
                configInfoAggrEntity.setTenantId(tenantTmp);
                configInfoAggrEntity.setDatumId(datumId);
                configInfoAggrEntity.setAppName(appNameTmp);
                configInfoAggrEntity.setContent(content);
                configInfoAggrEntity.setGmtModified(LocalDateTime.now());
                return configInfoAggrEntityMapper.insert(configInfoAggrEntity) > 0;
            }
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public boolean batchPublishAggr(final String dataId, final String group, final String tenant, final Map<String, String> datumMap, final String appName) {
        try {
            Boolean isPublishOk = tjt.execute(status -> {
                for (Map.Entry<String, String> entry : datumMap.entrySet()) {
                    try {
                        if (!addAggrConfigInfo(dataId, group, tenant, entry.getKey(), appName, entry.getValue())) {
                            throw new TransactionSystemException("error in batchPublishAggr");
                        }
                    } catch (Throwable e) {
                        throw new TransactionSystemException("error in batchPublishAggr");
                    }
                }
                return Boolean.TRUE;
            });
            if (isPublishOk == null) {
                return false;
            }
            return isPublishOk;
        } catch (TransactionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            return false;
        }
    }

    @Override
    public boolean replaceAggr(final String dataId, final String group, final String tenant, final Map<String, String> datumMap, final String appName) {
        try {
            Boolean isReplaceOk = tjt.execute(status -> {
                try {
                    String appNameTmp = appName == null ? "" : appName;
                    removeAggrConfigInfo(dataId, group, tenant);
                    String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
                    for (Map.Entry<String, String> datumEntry : datumMap.entrySet()) {
                        ConfigInfoAggrEntity configInfoAggrEntity = new ConfigInfoAggrEntity();
                        configInfoAggrEntity.setDataId(dataId);
                        configInfoAggrEntity.setGroupId(group);
                        configInfoAggrEntity.setTenantId(tenantTmp);
                        configInfoAggrEntity.setDatumId(datumEntry.getKey());
                        configInfoAggrEntity.setAppName(appNameTmp);
                        configInfoAggrEntity.setContent(datumEntry.getValue());
                        configInfoAggrEntity.setGmtModified(LocalDateTime.now());
                        configInfoAggrEntityMapper.insert(configInfoAggrEntity);
                    }
                } catch (Throwable e) {
                    throw new TransactionSystemException("error in addAggrConfigInfo");
                }
                return Boolean.TRUE;
            });
            if (isReplaceOk == null) {
                return false;
            }
            return isReplaceOk;
        } catch (TransactionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            return false;
        }

    }

    @Override
    public void removeSingleAggrConfigInfo(final String dataId, final String group, final String tenant, final String datumId) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        QueryWrapper queryWrapper = QueryWrapper.create();
        concatEq(queryWrapper, ConfigInfoAggrEntity::getDataId, dataId);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getGroupId, group);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getTenantId, tenantTmp);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getDatumId, datumId);

        try {
            configInfoAggrEntityMapper.deleteByQuery(queryWrapper);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public void removeAggrConfigInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        QueryWrapper queryWrapper = QueryWrapper.create();
        concatEq(queryWrapper, ConfigInfoAggrEntity::getDataId, dataId);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getGroupId, group);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getTenantId, tenantTmp);

        try {
            configInfoAggrEntityMapper.deleteByQuery(queryWrapper);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public boolean batchRemoveAggr(final String dataId, final String group, final String tenant, final List<String> datumList) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        QueryWrapper queryWrapper = QueryWrapper.create();
        concatEq(queryWrapper, ConfigInfoAggrEntity::getDataId, dataId);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getGroupId, group);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getTenantId, tenantTmp);
        concatIn(queryWrapper, ConfigInfoAggrEntityTableDef.CONFIG_INFO_AGGR_ENTITY.DATUM_ID, datumList);

        try {
            configInfoAggrEntityMapper.deleteByQuery(queryWrapper);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            return false;
        }
        return true;
    }

    @Override
    public int aggrConfigInfoCount(String dataId, String group, String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        QueryWrapper queryWrapper = QueryWrapper.create();
        concatEq(queryWrapper, ConfigInfoAggrEntity::getDataId, dataId);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getGroupId, group);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getTenantId, tenantTmp);

        return (int) configInfoAggrEntityMapper.selectCountByQuery(queryWrapper);
    }

    @Override
    public int aggrConfigInfoCount(String dataId, String group, String tenant, List<String> datumIds, boolean isIn) {
        if (datumIds == null || datumIds.isEmpty()) {
            return 0;
        }
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        QueryWrapper queryWrapper = QueryWrapper.create();
        concatEq(queryWrapper, ConfigInfoAggrEntity::getDataId, dataId);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getGroupId, group);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getTenantId, tenantTmp);

        if (isIn) {
            concatIn(queryWrapper, ConfigInfoAggrEntityTableDef.CONFIG_INFO_AGGR_ENTITY.DATUM_ID, datumIds);
        } else {
            concatNotIn(queryWrapper, ConfigInfoAggrEntityTableDef.CONFIG_INFO_AGGR_ENTITY.DATUM_ID, datumIds);
        }

        return (int) configInfoAggrEntityMapper.selectCountByQuery(queryWrapper);
    }

    @Override
    public ConfigInfoAggr findSingleConfigInfoAggr(String dataId, String group, String tenant, String datumId) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        QueryWrapper queryWrapper = QueryWrapper.create().select(ConfigInfoAggrEntity::getId).select(ConfigInfoAggrEntity::getDataId).select(ConfigInfoAggrEntity::getGroupId).select(ConfigInfoAggrEntity::getTenantId).select(ConfigInfoAggrEntity::getDatumId).select(ConfigInfoAggrEntity::getAppName).select(ConfigInfoAggrEntity::getContent);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getDataId, dataId);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getGroupId, group);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getTenantId, tenantTmp);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getDatumId, datumId);

        try {
            return configInfoAggrEntityMapper.selectOneByQueryAs(queryWrapper, ConfigInfoAggr.class);
        } catch (EmptyResultDataAccessException e) {
            // EmptyResultDataAccessException, indicating that the data does not exist, returns null
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        } catch (Exception e) {
            LogUtil.FATAL_LOG.error("[db-other-error]" + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<ConfigInfoAggr> findConfigInfoAggr(String dataId, String group, String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        QueryWrapper queryWrapper = QueryWrapper.create().select(ConfigInfoAggrEntity::getDataId).select(ConfigInfoAggrEntity::getGroupId).select(ConfigInfoAggrEntity::getTenantId).select(ConfigInfoAggrEntity::getDatumId).select(ConfigInfoAggrEntity::getAppName).select(ConfigInfoAggrEntity::getContent);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getDataId, dataId);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getGroupId, group);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getTenantId, tenantTmp);
        queryWrapper.orderBy(ConfigInfoAggrEntity::getDatumId);

        try {
            return configInfoAggrEntityMapper.selectListByQueryAs(queryWrapper, ConfigInfoAggr.class);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        } catch (EmptyResultDataAccessException e) {
            return Collections.emptyList();
        } catch (Exception e) {
            LogUtil.FATAL_LOG.error("[db-other-error]" + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Page<ConfigInfoAggr> findConfigInfoAggrByPage(String dataId, String group, String tenant, final int pageNo, final int pageSize) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;

        QueryWrapper queryWrapper = QueryWrapper.create().select(ConfigInfoAggrEntity::getDataId).select(ConfigInfoAggrEntity::getGroupId).select(ConfigInfoAggrEntity::getTenantId).select(ConfigInfoAggrEntity::getDatumId).select(ConfigInfoAggrEntity::getAppName).select(ConfigInfoAggrEntity::getContent);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getDataId, dataId);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getGroupId, group);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getTenantId, tenantTmp);
        queryWrapper.orderBy(ConfigInfoAggrEntity::getDatumId);
        try {
            return convertPage(configInfoAggrEntityMapper.paginateAs(pageNo, pageSize, queryWrapper, ConfigInfoAggr.class));
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public Page<ConfigInfoAggr> findConfigInfoAggrLike(final int pageNo, final int pageSize, ConfigKey[] configKeys, boolean blacklist) {
        // Whitelist, please leave the synchronization condition empty, there is no configuration that meets the conditions
        if (configKeys.length == 0 && blacklist == false) {
            Page<ConfigInfoAggr> page = new Page<>();
            page.setTotalCount(0);
            return page;
        }

        QueryChain<ConfigInfoAggrEntity> queryChain = QueryChain.of(ConfigInfoAggrEntity.class).select(ConfigInfoAggrEntity::getDataId).select(ConfigInfoAggrEntity::getGroupId).select(ConfigInfoAggrEntity::getTenantId).select(ConfigInfoAggrEntity::getDatumId).select(ConfigInfoAggrEntity::getAppName).select(ConfigInfoAggrEntity::getContent);

        for (ConfigKey configInfoAggr : configKeys) {
            String dataId = configInfoAggr.getDataId();
            String group = configInfoAggr.getGroup();
            String appName = configInfoAggr.getAppName();
            if (StringUtils.isBlank(dataId) && StringUtils.isBlank(group) && StringUtils.isBlank(appName)) {
                break;
            }
            if (blacklist) {
                QueryCondition queryCondition = QueryCondition.createEmpty();
                if (StringUtils.isNotBlank(dataId)) {
                    queryCondition.or(ConfigInfoAggrEntityTableDef.CONFIG_INFO_AGGR_ENTITY.DATA_ID.notLike(dataId));
                }
                if (StringUtils.isNotBlank(group)) {
                    queryCondition.or(ConfigInfoAggrEntityTableDef.CONFIG_INFO_AGGR_ENTITY.GROUP_ID.notLike(group));
                }
                if (StringUtils.isNotBlank(appName)) {
                    queryCondition.or(ConfigInfoAggrEntityTableDef.CONFIG_INFO_AGGR_ENTITY.APP_NAME.ne(group));
                }
                queryChain.and(queryCondition);
            } else {
                QueryCondition queryCondition = QueryCondition.createEmpty();
                if (StringUtils.isNotBlank(dataId)) {
                    queryCondition.and(ConfigInfoAggrEntityTableDef.CONFIG_INFO_AGGR_ENTITY.DATA_ID.like(dataId));
                }
                if (StringUtils.isNotBlank(group)) {
                    queryCondition.and(ConfigInfoAggrEntityTableDef.CONFIG_INFO_AGGR_ENTITY.GROUP_ID.like(group));
                }
                if (!StringUtils.isBlank(appName)) {
                    queryCondition.and(ConfigInfoAggrEntityTableDef.CONFIG_INFO_AGGR_ENTITY.APP_NAME.eq(appName));
                }
                queryChain.or(queryCondition);
            }
        }

        try {
            return convertPage(configInfoAggrEntityMapper.paginateAs(pageNo, pageSize, queryChain, ConfigInfoAggr.class));
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public List<ConfigInfoChanged> findAllAggrGroup() {
        QueryWrapper queryWrapper = QueryChain.of(ConfigInfoAggrEntity.class).select(QueryMethods.distinct(ConfigInfoAggrEntity::getDataId)).select(ConfigInfoAggrEntity::getGroupId).select(ConfigInfoAggrEntity::getTenantId);
        try {
            return configInfoAggrEntityMapper.selectListByQueryAs(queryWrapper, ConfigInfoChanged.class);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (Exception e) {
            LogUtil.FATAL_LOG.error("[db-other-error]" + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> findDatumIdByContent(String dataId, String groupId, String content) {
        QueryWrapper queryWrapper = QueryWrapper.create().select(ConfigInfoAggrEntity::getDatumId);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getDataId, dataId);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getGroupId, groupId);
        concatEq(queryWrapper, ConfigInfoAggrEntity::getContent, content);
        try {
            return configInfoAggrEntityMapper.selectListByQueryAs(queryWrapper, String.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (IncorrectResultSizeDataAccessException e) {
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }
}
