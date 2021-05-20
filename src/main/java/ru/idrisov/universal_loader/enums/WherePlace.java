package ru.idrisov.universal_loader.enums;

public enum WherePlace {
    BEFORE_JOIN,
    AFTER_JOIN,

    //TODO проверить работоспособность(на 95 процентов она не работает)
    @Deprecated
    AFTER_AGGREGATE
}
