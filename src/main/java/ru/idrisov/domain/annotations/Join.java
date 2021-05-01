package ru.idrisov.domain.annotations;

import ru.idrisov.domain.entitys.TableSpark;
import ru.idrisov.domain.enums.JoinTypes;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface Join {
    JoinTypes type() default JoinTypes.LEFT;
    Class<? extends TableSpark> mainTable();
    Class<? extends TableSpark> joinedTable();
    JoinCondition[] joinCondition() default {};
}
