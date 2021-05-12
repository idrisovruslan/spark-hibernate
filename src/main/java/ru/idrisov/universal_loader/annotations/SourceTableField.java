package ru.idrisov.universal_loader.annotations;

import ru.idrisov.universal_loader.entitys.TableSpark;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface SourceTableField {
    Class<? extends TableSpark> sourceTable();
    String sourceFieldName();
    WhereCondition[] conditions() default {};
}
