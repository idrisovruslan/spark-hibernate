package ru.idrisov.domain.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface EntitySpark {
    //TODO придумать механизм стандартизации
    String tableSchema();
    String tableName();
    boolean filling();
}
