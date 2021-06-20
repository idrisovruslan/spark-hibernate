package ru.idrisov.universal_loader.annotations;

import ru.idrisov.universal_loader.enums.ColumnValue;
import ru.idrisov.universal_loader.enums.ConditionType;
import ru.idrisov.universal_loader.enums.WherePlace;

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
    String cycleRightValue() default "";

    String leftValueFunction() default "%s";
    String leftFieldName() default "";

    WherePlace place() default WherePlace.BEGINNING;
    int orGroup() default -1;
}
