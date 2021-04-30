package ru.idrisov.domain.annotations;

import ru.idrisov.domain.entitys.TableSpark;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface SourceTableField {
    Class<? extends TableSpark> sourceTable();
    String sourceFieldName();
}
