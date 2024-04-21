package com.alibaba.nacos.plugin.auth.impl.persistence;

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.config.server.configuration.ConditionalOnUseOrm;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.repository.mybatisflex.MybatisFlexUtils;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.plugin.auth.impl.mapper.UserEntityMapper;
import com.github.pagehelper.page.PageMethod;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConditionalOnUseOrm("mybatis-flex")
public class MybatisFlexUserPersistServiceImpl implements UserPersistService {

    @Autowired
    private UserEntityMapper userEntityMapper;

    private static final String PATTERN_STR = "*";

    /**
     * Execute create user operation.
     *
     * @param username username string value.
     * @param password password string value.
     */
    @Override
    public void createUser(String username, String password) {
        UserEntity userEntity = new UserEntity();
        userEntity.setUsername(username);
        userEntity.setPassword(password);
        userEntity.setEnabled(true);
        try {
            userEntityMapper.insert(userEntity);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * Execute delete user operation.
     *
     * @param username username string value.
     */
    @Override
    public void deleteUser(String username) {
        try {
            userEntityMapper.deleteById(username);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * Execute update user password operation.
     *
     * @param username username string value.
     * @param password password string value.
     */
    @Override
    public void updateUserPassword(String username, String password) {
        try {
            UserEntity userEntity = new UserEntity();
            userEntity.setUsername(username);
            userEntity.setPassword(password);
            userEntityMapper.update(userEntity);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    /**
     * Execute find user by username operation.
     *
     * @param username username string value.
     * @return User model.
     */
    @Override
    public User findUserByUsername(String username) {
        try {
            return userEntityMapper.findUserByUsername(username);
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
    public Page<User> getUsers(int pageNo, int pageSize, String username) {
        try (com.github.pagehelper.Page<User> pageInfo = PageMethod.startPage(pageNo, pageSize)) {
            userEntityMapper.getUsers(username);
            return MybatisFlexUtils.convertPage(pageInfo);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }

    @Override
    public List<String> findUserLikeUsername(String username) {
        return userEntityMapper.findUserLikeUsername(String.format("%%%s%%", username));
    }

    @Override
    public Page<User> findUsersLike4Page(String username, int pageNo, int pageSize) {
        try (com.github.pagehelper.Page<User> pageInfo = PageMethod.startPage(pageNo, pageSize)) {
            if (StringUtils.isNotBlank(username)) {
                userEntityMapper.findUsersLike(generateLikeArgument(username));
            } else {
                userEntityMapper.findUsersLike(null);
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
