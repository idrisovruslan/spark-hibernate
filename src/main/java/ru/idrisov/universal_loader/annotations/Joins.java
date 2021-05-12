package ru.idrisov.universal_loader.annotations;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Inherited
public @interface Joins {
    Join[] joins();
}
