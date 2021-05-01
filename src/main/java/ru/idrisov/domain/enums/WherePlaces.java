package ru.idrisov.domain.enums;

public enum WherePlaces {
    BEFORE("Before"),
    AFTER("After");

    private final String wherePlaces;

    WherePlaces(String wherePlaces) {
        this.wherePlaces = wherePlaces;
    }

    public String getWherePlaces() {
        return wherePlaces;
    }
}
