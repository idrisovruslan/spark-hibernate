package ru.idrisov.domain.annotations;

import ru.idrisov.domain.enums.ConditionType;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface JoinCondition {
    ConditionType type() default ConditionType.EQUAL_TO;
    String mainTableField();
    String joinedTableField();
}
