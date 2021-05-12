package ru.idrisov.universal_loader.annotations;

import ru.idrisov.universal_loader.enums.AggregateFunctions;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Aggregate {
    AggregateFunctions function();
}
