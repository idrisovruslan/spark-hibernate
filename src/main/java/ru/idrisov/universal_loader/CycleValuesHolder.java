package ru.idrisov.universal_loader;

import lombok.Getter;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CycleValuesHolder {
    CycleValuesCreator cycleValuesCreator;
    List<CycleValue> cycleValues;
    @Getter
    CycleValue currentCycleValue;

    public CycleValuesHolder(CycleValuesCreator cycleValuesCreator) {
        this.cycleValuesCreator = cycleValuesCreator;
        cycleValues = this.cycleValuesCreator.getCycleValuesList();
    }
}
