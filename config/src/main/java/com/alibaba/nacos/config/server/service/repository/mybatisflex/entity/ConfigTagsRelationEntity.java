package com.alibaba.nacos.config.server.service.repository.mybatisflex.entity;

import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.mybatisflex.annotation.Table;
import lombok.Data;

@Data
@Table(TableConstant.CONFIG_TAGS_RELATION)
public class ConfigTagsRelationEntity {

    private Long id;

    private String tagName;

    private String tagType;

    private String dataId;

    private String groupId;

    private String tenantId;

    private String nid;

}
