package ru.idrisov.universal_loader;

import lombok.Getter;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
//TODO Я хз нахер он сейчас нужен, но пусть уж полежит, пока не придумаю :)
public class CycleValuesHolder {
    CycleValuesCreator cycleValuesCreator;
    List<CycleValue> cycleValues;

    @Getter
    String currentMainCycleValue;
    @Getter
    String currentNestedCycleValue;

    public CycleValuesHolder(CycleValuesCreator cycleValuesCreator) {
//        this.cycleValuesCreator = cycleValuesCreator;
//        cycleValues = this.cycleValuesCreator.getCycleValuesList();
    }


}
