package com.alibaba.nacos.plugin.auth.impl.mapper;

import com.alibaba.nacos.plugin.auth.impl.persistence.User;
import com.alibaba.nacos.plugin.auth.impl.persistence.UserEntity;
import com.mybatisflex.core.BaseMapper;

import java.util.List;

public interface UserEntityMapper extends BaseMapper<UserEntity> {

    User findUserByUsername(String username);

    List<User> getUsers(String username);

    List<String> findUserLikeUsername(String username);

    List<User> findUsersLike(String username);

}