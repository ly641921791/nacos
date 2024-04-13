package com.alibaba.nacos.config.server.service.repository.mybatisflex.entity;

import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.mybatisflex.annotation.Table;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Table(TableConstant.CONFIG_INFO)
public class ConfigInfoEntity {

    private Long id;

    private String dataId;

    private String groupId;

    private String tenantId;

    private String appName;

    private String content;

    private String md5;

    private LocalDateTime gmtCreate;

    private LocalDateTime gmtModified;

    private String srcUser;

    private String srcIp;

    private String cDesc;

    private String cUse;

    private String effect;

    private String type;

    private String cSchema;

    private String encryptedDataKey;

}
