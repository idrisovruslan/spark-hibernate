package ru.idrisov.domain.annotations;

import ru.idrisov.domain.enums.AggregateFunctions;

public @interface Aggregate {
    AggregateFunctions function();
}
