package com.alibaba.nacos.config.server.service.repository.mybatisflex;

import com.alibaba.nacos.config.server.configuration.ConditionalOnUseOrm;
import com.mybatisflex.spring.boot.MybatisFlexAutoConfiguration;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MybatisFlexConfig {

    @Configuration
    @ConditionalOnUseOrm("mybatis-flex")
    @ComponentScan("com.alibaba.nacos.config.server.service.repository.mybatisflex")
    @MapperScan("com.alibaba.nacos.config.server.service.repository.mybatisflex.mapper")
    public static class MybatisFlexEnableConfig {
    }

    @Configuration
    @ConditionalOnUseOrm("no")
    @EnableAutoConfiguration(exclude = MybatisFlexAutoConfiguration.class)
    public static class MybatisFlexDisableConfig {
    }

}
