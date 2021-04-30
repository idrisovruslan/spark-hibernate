package ru.idrisov.domain.annotations;

import org.apache.spark.sql.Column;
import ru.idrisov.domain.entitys.TableSpark;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface Condition {
    String type();
    String value();
    String valueType() default "String";
    String place();
}
