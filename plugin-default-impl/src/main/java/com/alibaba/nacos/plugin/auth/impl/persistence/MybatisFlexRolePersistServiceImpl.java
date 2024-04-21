package com.alibaba.nacos.plugin.auth.impl.persistence;

import com.alibaba.nacos.config.server.configuration.ConditionalOnUseOrm;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.MybatisFlexUtils;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.plugin.auth.impl.mapper.RoleEntityMapper;
import com.github.pagehelper.page.PageMethod;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConditionalOnUseOrm("mybatis-flex")
public class MybatisFlexRolePersistServiceImpl implements RolePersistService {

    @Autowired
    private RoleEntityMapper roleEntityMapper;

    private static final String PATTERN_STR = "*";

    @Override
    public Page<RoleInfo> getRoles(int pageNo, int pageSize) {
        return getRolesByUserNameAndRoleName(null, null ,pageNo ,pageSize);
    }

    @Override
    public Page<RoleInfo> getRolesByUserNameAndRoleName(String username, String role, int pageNo, int pageSize) {
        try (com.github.pagehelper.Page<RoleInfo> pageInfo = PageMethod.startPage(pageNo, pageSize)) {
            roleEntityMapper.getRolesByUserNameAndRoleName(username, role);
            return MybatisFlexUtils.convertPage(pageInfo);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * Execute add role operation.
     *
     * @param role     role string value.
     * @param userName username string value.
     */
    @Override
    public void addRole(String role, String userName) {
        RoleEntity roleEntity = new RoleEntity();
        roleEntity.setRole(role);
        roleEntity.setUsername(userName);
        try {
            roleEntityMapper.insert(roleEntity);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * Execute delete role operation.
     *
     * @param role role string value.
     */
    @Override
    public void deleteRole(String role) {
        try {
            roleEntityMapper.deleteByRole(role);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * Execute delete role operation.
     *
     * @param role     role string value.
     * @param username username string value.
     */
    @Override
    public void deleteRole(String role, String username) {
        try {
            roleEntityMapper.deleteRole(role, username);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    @Override
    public List<String> findRolesLikeRoleName(String role) {
        return roleEntityMapper.findRolesLikeRoleName(String.format("%%%s%%", role));
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

    @Override
    public Page<RoleInfo> findRolesLike4Page(String username, String role, int pageNo, int pageSize) {
        try (com.github.pagehelper.Page<RoleInfo> pageInfo = PageMethod.startPage(pageNo, pageSize)) {
            roleEntityMapper.findRolesLike(generateLikeArgument(username), generateLikeArgument(role));
            return MybatisFlexUtils.convertPage(pageInfo);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

}
