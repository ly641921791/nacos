package com.alibaba.nacos.plugin.auth.impl.configuration;

import com.alibaba.nacos.config.server.configuration.ConditionalOnUseOrm;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Configuration;

@ConditionalOnUseOrm("mybatis-flex")
@Configuration("AuthMybatisFlexConfig")
@MapperScan("com.alibaba.nacos.plugin.auth.impl.mapper")
public class MybatisFlexConfig {
}
