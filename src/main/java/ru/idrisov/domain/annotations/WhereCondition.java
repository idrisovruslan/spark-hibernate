package ru.idrisov.domain.annotations;

import ru.idrisov.domain.enums.ColumnValue;
import ru.idrisov.domain.enums.ConditionType;
import ru.idrisov.domain.enums.WherePlace;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface WhereCondition {
    ConditionType type() default ConditionType.EQUAL_TO;

    //TODO подумать над другим решением(например анатации для каждого типа)
    String stringRightValue() default "";
    ColumnValue columnRightValue() default ColumnValue.none;
    String[] arrayStringRightValue() default {};

    WherePlace place();
}
