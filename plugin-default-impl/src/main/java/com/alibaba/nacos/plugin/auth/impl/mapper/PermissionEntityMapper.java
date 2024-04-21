package com.alibaba.nacos.plugin.auth.impl.mapper;

import com.alibaba.nacos.plugin.auth.impl.persistence.PermissionEntity;
import com.alibaba.nacos.plugin.auth.impl.persistence.PermissionInfo;
import com.mybatisflex.core.BaseMapper;

import java.util.List;

public interface PermissionEntityMapper extends BaseMapper<PermissionEntity> {

    List<PermissionInfo> getPermissions(String role);

    void deletePermission(String role, String resource, String action);

    List<PermissionInfo> findPermissionsLike(String role);

}
