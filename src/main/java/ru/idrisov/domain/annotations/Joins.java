package ru.idrisov.domain.annotations;

import ru.idrisov.domain.entitys.TableSpark;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface Joins {
    Join[] joins();
}
