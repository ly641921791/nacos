package com.alibaba.nacos.plugin.auth.impl.persistence;

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.configuration.ConditionalOnUseOrm;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.MybatisFlexUtils;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.plugin.auth.impl.mapper.PermissionEntityMapper;
import com.github.pagehelper.page.PageMethod;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnUseOrm("mybatis-flex")
public class MybatisFlexPermissionPersistServiceImpl implements PermissionPersistService {

    @Autowired
    private PermissionEntityMapper permissionEntityMapper;

    private static final String PATTERN_STR = "*";
    
    @Override
    public Page<PermissionInfo> getPermissions(String role, int pageNo, int pageSize) {
        try (com.github.pagehelper.Page<PermissionInfo> pageInfo = PageMethod.startPage(pageNo, pageSize)) {
            permissionEntityMapper.getPermissions(role);
            return MybatisFlexUtils.convertPage(pageInfo);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * Execute add permission operation.
     *
     * @param role     role string value.
     * @param resource resource string value.
     * @param action   action string value.
     */
    @Override
    public void addPermission(String role, String resource, String action) {
        PermissionEntity permissionEntity = new PermissionEntity();
        permissionEntity.setRole(role);
        permissionEntity.setResource(resource);
        permissionEntity.setAction(action);
        try {
            permissionEntityMapper.insert(permissionEntity);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    /**
     * Execute delete permission operation.
     *
     * @param role     role string value.
     * @param resource resource string value.
     * @param action   action string value.
     */
    @Override
    public void deletePermission(String role, String resource, String action) {
        try {
            permissionEntityMapper.deletePermission(role, resource ,action);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    @Override
    public Page<PermissionInfo> findPermissionsLike4Page(String role, int pageNo, int pageSize) {
        try (com.github.pagehelper.Page<PermissionInfo> pageInfo = PageMethod.startPage(pageNo, pageSize)) {
            if (StringUtils.isNotBlank(role)) {
                permissionEntityMapper.findPermissionsLike(generateLikeArgument(role));
            } else {
                permissionEntityMapper.findPermissionsLike(null);
            }
            return MybatisFlexUtils.convertPage(pageInfo);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    @Override
    public String generateLikeArgument(String s) {
        String underscore = "_";
        if (s.contains(underscore)) {
            s = s.replaceAll(underscore, "\\\\_");
        }
        String fuzzySearchSign = "\\*";
        String sqlLikePercentSign = "%";
        if (s.contains(PATTERN_STR)) {
            return s.replaceAll(fuzzySearchSign, sqlLikePercentSign);
        } else {
            return s;
        }
    }

}
