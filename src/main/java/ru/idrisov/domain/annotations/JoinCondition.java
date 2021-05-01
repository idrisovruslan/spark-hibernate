package ru.idrisov.domain.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface JoinCondition {
    String type() default "equalTo";
    String mainTableField();
    String joinedField();
}
