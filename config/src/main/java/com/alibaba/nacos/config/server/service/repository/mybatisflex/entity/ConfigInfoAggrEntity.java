package com.alibaba.nacos.config.server.service.repository.mybatisflex.entity;

import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.mybatisflex.annotation.Table;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Table(TableConstant.CONFIG_INFO_AGGR)
public class ConfigInfoAggrEntity implements Serializable {

    private Long id;

    private String dataId;

    private String groupId;

    private String tenantId;

    private String datumId;

    private String appName;

    private String content;

    private LocalDateTime gmtModified;

}
