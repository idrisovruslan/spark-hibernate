package ru.idrisov.universal_loader.exceptions;

public class AllNestedCycleProcessedException extends Exception {
    public AllNestedCycleProcessedException(String msg) {
        super(msg);
    }
}
