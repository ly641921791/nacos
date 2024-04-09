package com.alibaba.nacos.config.server.service.repository.mybatisflex;

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.configuration.ConditionOnExternalStorage;
import com.alibaba.nacos.config.server.model.TenantInfo;
import com.alibaba.nacos.config.server.service.repository.CommonPersistService;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.entity.TenantInfoEntity;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.mapper.TenantInfoEntityMapper;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.mybatisflex.core.query.QueryChain;
import com.mybatisflex.core.query.QueryWrapper;
import com.mybatisflex.core.util.LambdaGetter;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * ExternalOtherPersistServiceImpl.
 *
 * @author lixiaoshuang
 */
@SuppressWarnings(value = {"PMD.MethodReturnWrapperTypeRule", "checkstyle:linelength"})
@Conditional(value = ConditionOnExternalStorage.class)
@Service
public class MybatisFlexCommonPersistServiceImpl implements CommonPersistService {

    @Autowired
    protected JdbcTemplate jt;
    @Autowired
    private TenantInfoEntityMapper tenantInfoEntityMapper;

    @Override
    public void insertTenantInfoAtomic(String kp, String tenantId, String tenantName, String tenantDesc, String createResoure, final long time) {
        try {
            TenantInfoEntity tenantInfoEntity = new TenantInfoEntity();
            tenantInfoEntity.setKp(kp);
            tenantInfoEntity.setTenantId(tenantId);
            tenantInfoEntity.setTenantName(tenantName);
            tenantInfoEntity.setTenantDesc(tenantDesc);
            tenantInfoEntity.setCreateSource(createResoure);
            tenantInfoEntity.setGmtCreate(time);
            tenantInfoEntity.setGmtModified(time);
            tenantInfoEntityMapper.insert(tenantInfoEntity);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public void removeTenantInfoAtomic(final String kp, final String tenantId) {
        try {
            QueryWrapper queryWrapper = QueryWrapper.create();
            concatEq(queryWrapper, TenantInfoEntity::getKp, kp);
            concatEq(queryWrapper, TenantInfoEntity::getTenantId, tenantId);
            tenantInfoEntityMapper.deleteByQuery(queryWrapper);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public void updateTenantNameAtomic(String kp, String tenantId, String tenantName, String tenantDesc) {
        try {
            TenantInfoEntity tenantInfoEntity = new TenantInfoEntity();
            tenantInfoEntity.setTenantName(tenantName);
            tenantInfoEntity.setTenantDesc(tenantDesc);
            tenantInfoEntity.setGmtModified(System.currentTimeMillis());

            QueryChain<TenantInfoEntity> queryChain = QueryChain.of(TenantInfoEntity.class);
            concatEq(queryChain, TenantInfoEntity::getKp, kp);
            concatEq(queryChain, TenantInfoEntity::getTenantId, tenantId);

            tenantInfoEntityMapper.updateByQuery(tenantInfoEntity, queryChain);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        }
    }

    @Override
    public List<TenantInfo> findTenantByKp(String kp) {
        QueryWrapper queryWrapper = QueryWrapper.create().select(TenantInfoEntity::getTenantId).select(TenantInfoEntity::getTenantName).select(TenantInfoEntity::getTenantDesc);
        concatEq(queryWrapper, TenantInfoEntity::getKp, kp);
        try {
            return tenantInfoEntityMapper.selectListByQuery(queryWrapper).stream().map(tenantInfoEntity -> {
                TenantInfo tenantInfo = new TenantInfo();
                BeanUtils.copyProperties(tenantInfoEntity, tenantInfo);
                return tenantInfo;
            }).collect(Collectors.toList());
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
    public TenantInfo findTenantByKp(String kp, String tenantId) {
        QueryWrapper queryWrapper = QueryWrapper.create().select(TenantInfoEntity::getTenantId).select(TenantInfoEntity::getTenantName).select(TenantInfoEntity::getTenantDesc);
        concatEq(queryWrapper, TenantInfoEntity::getKp, kp);
        concatEq(queryWrapper, TenantInfoEntity::getTenantId, tenantId);
        try {
            TenantInfoEntity tenantInfoEntity = tenantInfoEntityMapper.selectOneByQuery(queryWrapper);
            TenantInfo tenantInfo = new TenantInfo();
            BeanUtils.copyProperties(tenantInfoEntity, tenantInfo);
            return tenantInfo;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e, e);
            throw e;
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (Exception e) {
            LogUtil.FATAL_LOG.error("[db-other-error]" + e.getMessage(), e);
            throw new RuntimeException(e);
        }
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
    public boolean isExistTable(String tableName) {
        String sql = String.format("SELECT 1 FROM %s LIMIT 1", tableName);
        try {
            jt.queryForObject(sql, Integer.class);
            return true;
        } catch (Throwable e) {
            return false;
        }
    }

    @Override
    public int tenantInfoCountByTenantId(String tenantId) {
        if (Objects.isNull(tenantId)) {
            throw new IllegalArgumentException("tenantId can not be null");
        }

        QueryWrapper queryWrapper = QueryWrapper.create();
        concatEq(queryWrapper, TenantInfoEntity::getTenantId, tenantId);
        return (int) tenantInfoEntityMapper.selectCountByQuery(queryWrapper);
    }

    private <T> void concatEq(QueryWrapper queryChain, LambdaGetter<T> column, String value) {
        if (StringUtils.isBlank(value)) {
            queryChain.isNull(column);
        } else {
            queryChain.eq(column, value);
        }
    }

    private <R, T> void concatEq(QueryChain<R> queryChain, LambdaGetter<T> column, String value) {
        concatEq((QueryWrapper) queryChain, column, value);
    }

}
