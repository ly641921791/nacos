package com.alibaba.nacos.plugin.auth.impl.persistence;

import com.mybatisflex.annotation.Id;
import com.mybatisflex.annotation.Table;
import lombok.Data;

import java.io.Serializable;

@Data
@Table("users")
public class UserEntity implements Serializable {

    @Id
    private String username;
    
    private String password;

    private Boolean enabled;

}
