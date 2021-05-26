package ru.idrisov.universal_loader.annotations;

import ru.idrisov.universal_loader.entitys.TableSpark;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Cycles.class)
public @interface Cycle {
    Class<? extends TableSpark> table();
    String column();
    String cycleName();
    WhereCondition[] conditions() default {};
    String nestedCycleName() default "";
}
