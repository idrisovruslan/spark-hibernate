package ru.idrisov.universal_loader.annotations;

import ru.idrisov.universal_loader.enums.ConditionType;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface JoinCondition {
    ConditionType type() default ConditionType.EQUAL_TO;
    String mainTableField();
    String mainTableFunction() default "%s";
    String joinedTableField();
    String joinedTableFunction() default "%s";

    int orGroup() default -1;
}
