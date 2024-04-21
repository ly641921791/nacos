package com.alibaba.nacos.plugin.auth.impl.persistence;

import com.mybatisflex.annotation.Table;
import lombok.Data;

import java.io.Serializable;

@Data
@Table("roles")
public class RoleEntity implements Serializable {

    private String role;

    private String username;

}
