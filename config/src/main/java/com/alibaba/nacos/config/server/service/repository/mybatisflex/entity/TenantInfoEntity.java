package com.alibaba.nacos.config.server.service.repository.mybatisflex.entity;

import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.mybatisflex.annotation.Table;
import lombok.Data;

import java.io.Serializable;

@Data
@Table(TableConstant.TENANT_INFO)
public class TenantInfoEntity implements Serializable {

    private String kp;

    private String tenantId;

    private String tenantName;

    private String tenantDesc;

    private String createSource;

    private Long gmtCreate;

    private Long gmtModified;

}
