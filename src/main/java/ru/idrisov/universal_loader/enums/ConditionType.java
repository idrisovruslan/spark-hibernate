package ru.idrisov.universal_loader.enums;

public enum ConditionType {
    //NONE("NONE"),
    IS_IN("%s IN (%s)"),
    EQUAL_TO("%s = %s"),
    IS_NULL("%s IS NULL"),
    LEQ("%s <= %s"),
    LT("%s < %s"),
    GEQ("%s => %s"),
    GT("%s > %s");

    private final String conditionFunction;

    ConditionType(String conditionFunction) {
        this.conditionFunction = conditionFunction;
    }

    public String getConditionFunction() {
        return conditionFunction;
    }
}
