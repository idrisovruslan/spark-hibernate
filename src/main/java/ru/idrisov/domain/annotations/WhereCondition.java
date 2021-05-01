package ru.idrisov.domain.annotations;

import ru.idrisov.domain.enums.WherePlaces;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface WhereCondition {
    String type() default "equalTo";
    String value();
    String valueType() default "String";
    WherePlaces place();
}
