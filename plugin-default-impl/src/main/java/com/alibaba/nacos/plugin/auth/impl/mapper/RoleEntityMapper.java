package com.alibaba.nacos.plugin.auth.impl.mapper;

import com.alibaba.nacos.plugin.auth.impl.persistence.RoleEntity;
import com.alibaba.nacos.plugin.auth.impl.persistence.RoleInfo;
import com.mybatisflex.core.BaseMapper;

import java.util.List;

public interface RoleEntityMapper extends BaseMapper<RoleEntity> {

    List<RoleInfo> getRolesByUserNameAndRoleName(String username, String role);

    void deleteByRole(String role);

    void deleteRole(String role, String username);

    List<String> findRolesLikeRoleName(String role);

    List<RoleInfo> findRolesLike(String username, String role);

}