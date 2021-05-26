package ru.idrisov.universal_loader;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.idrisov.universal_loader.entitys.TableSpark;

import java.util.List;

@Service
@RequiredArgsConstructor
public class CycleValuesHolder {

    final CycleValuesCreator cycleValuesCreator;

    List<CycleValue> cycleValues;

    CycleValue currentCycleValue;


    void init(Class<? extends TableSpark> tableInfo) {
        cycleValues = cycleValuesCreator.getCycleValuesList(tableInfo);
    }


}
