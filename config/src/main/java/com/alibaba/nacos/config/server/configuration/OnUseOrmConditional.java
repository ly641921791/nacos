package com.alibaba.nacos.config.server.configuration;

import org.springframework.boot.autoconfigure.condition.ConditionMessage;
import org.springframework.boot.autoconfigure.condition.ConditionOutcome;
import org.springframework.boot.autoconfigure.condition.SpringBootCondition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotationPredicates;
import org.springframework.core.type.AnnotatedTypeMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class OnUseOrmConditional extends SpringBootCondition {

    @Override
    public ConditionOutcome getMatchOutcome(ConditionContext context, AnnotatedTypeMetadata metadata) {
        List<AnnotationAttributes> allAnnotationAttributes = metadata.getAnnotations()
                .stream(ConditionalOnUseOrm.class.getName())
                .filter(MergedAnnotationPredicates.unique(MergedAnnotation::getMetaTypes))
                .map(MergedAnnotation::asAnnotationAttributes).collect(Collectors.toList());

        String ormType = context.getEnvironment().getProperty("db.orm.type", String.class);

        List<ConditionMessage> noMatch = new ArrayList<>();
        List<ConditionMessage> match = new ArrayList<>();
        for (AnnotationAttributes annotationAttributes : allAnnotationAttributes) {
            ConditionOutcome outcome = determineOutcome(annotationAttributes, ormType);
            (outcome.isMatch() ? match : noMatch).add(outcome.getConditionMessage());
        }
        if (!noMatch.isEmpty()) {
            return ConditionOutcome.noMatch(ConditionMessage.of(noMatch));
        }
        return ConditionOutcome.match(ConditionMessage.of(match));
    }

    private ConditionOutcome determineOutcome(AnnotationAttributes annotationAttributes, String destOrmType) {
        String ormType = annotationAttributes.getString("ormType");
        if (Objects.equals(ormType, destOrmType)) {
            return ConditionOutcome.match(ConditionMessage.forCondition(ConditionalOnUseOrm.class,
                    String.format("destOrmType is %s, ormType is %s", destOrmType, ormType)).because("matched"));
        } else {
            return ConditionOutcome.noMatch(ConditionMessage.forCondition(ConditionalOnUseOrm.class,
                    String.format("destOrmType is %s, ormType is %s", destOrmType, ormType)).because("no matched"));
        }
    }

}
