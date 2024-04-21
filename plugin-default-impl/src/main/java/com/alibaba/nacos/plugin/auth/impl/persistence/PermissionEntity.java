package com.alibaba.nacos.plugin.auth.impl.persistence;

import com.mybatisflex.annotation.Table;
import lombok.Data;

import java.io.Serializable;

@Data
@Table("permissions")
public class PermissionEntity implements Serializable {

    /**
     * Role name.
     */
    private String role;

    /**
     * Resource.
     */
    private String resource;

    /**
     * Action on resource.
     */
    private String action;

}
