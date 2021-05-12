package ru.idrisov.universal_loader.annotations;

import ru.idrisov.universal_loader.entitys.TableSpark;
import ru.idrisov.universal_loader.enums.JoinTypes;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({})
public @interface Join {
    JoinTypes joinType() default JoinTypes.LEFT;
    Class<? extends TableSpark> mainTable();
    Class<? extends TableSpark> joinedTable();
    JoinCondition[] joinCondition() default {};
}
