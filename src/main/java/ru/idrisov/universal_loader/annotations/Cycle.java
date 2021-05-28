package ru.idrisov.universal_loader.annotations;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Cycles.class)
public @interface Cycle {
    String cycleName();
    String nestedCycleName() default "";
    SourceTableField sourceTableField();
}
