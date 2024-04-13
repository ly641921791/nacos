package com.alibaba.nacos.config.server.service.repository.mybatisflex.entity;

import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.mybatisflex.annotation.Table;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Table(TableConstant.CONFIG_INFO_BETA)
public class ConfigInfoBetaEntity implements Serializable {

    private Long id;

    private String dataId;

    private String groupId;

    private String tenantId;

    private String appName;

    private String content;

    private String betaIps;

    private String md5;

    private LocalDateTime gmtCreate;

    private LocalDateTime gmtModified;

    private String srcUser;

    private String srcIp;

    private String encryptedDataKey;

}
