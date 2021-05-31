package ru.idrisov.universal_loader;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.idrisov.universal_loader.entitys.TableSpark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class CycleValuesHolder {

    final CycleValuesCreator cycleValuesCreator;

    List<CycleValue> cycleValues;

    CycleValue currentCycleValue;


    public void init(Class<? extends TableSpark> tableInfo) {
        cycleValues = cycleValuesCreator.getCycleValuesList(tableInfo);
    }

    private Map<String, String> getNextValues(CycleValue cycleValue) {
        Map<String, String> result = new HashMap<>();
        result.put(cycleValue.getMainCycleName(), cycleValue.getMainCycleValue());

        while (cycleValue.nestedCycleIsPresent()) {
            CycleValue tempCycleValue = cycleValue.getFirstNestedCycleValueAndRemoveIfLast();
            if (tempCycleValue == null) {
                continue;
            }
            cycleValue = tempCycleValue;

            result.put(cycleValue.getMainCycleName(), cycleValue.getMainCycleValue());
        }

        return result;
    }

}
