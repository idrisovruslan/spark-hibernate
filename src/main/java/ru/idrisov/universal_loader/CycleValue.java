package ru.idrisov.universal_loader;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
public class CycleValue {
    final String mainCycleName;
    final String mainCycleValue;
    @Setter
    List<CycleValue> nestedCycleValue;

    public CycleValue(String mainCycleName, String mainCycleValue) {
        this.mainCycleName = mainCycleName;
        this.mainCycleValue = mainCycleValue;
    }
}
