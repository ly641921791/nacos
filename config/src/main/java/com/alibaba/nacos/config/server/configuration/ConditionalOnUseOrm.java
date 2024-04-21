package com.alibaba.nacos.config.server.configuration;

import org.springframework.context.annotation.Conditional;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
@Documented
@Conditional(OnUseOrmConditional.class)
public @interface ConditionalOnUseOrm {

    @AliasFor("ormType")
    String value() default "";

    @AliasFor("value")
    String ormType() default "";

}
