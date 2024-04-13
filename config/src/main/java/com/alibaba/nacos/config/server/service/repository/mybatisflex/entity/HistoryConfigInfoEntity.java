package com.alibaba.nacos.config.server.service.repository.mybatisflex.entity;

import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.mybatisflex.annotation.Table;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Table(TableConstant.HIS_CONFIG_INFO)
public class HistoryConfigInfoEntity {

    private Long id;

    private Long nid;

    private String dataId;

    private String groupId;

    private String appName;

    private String content;

    private String md5;

    private LocalDateTime gmtCreate;

    private LocalDateTime gmtModified;

    private String srcUser;

    private String srcIp;

    private String opType;

    private String tenantId;

    private String encryptedDataKey;

}
