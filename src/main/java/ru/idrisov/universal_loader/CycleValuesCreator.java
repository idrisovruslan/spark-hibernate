package ru.idrisov.universal_loader;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class CycleValuesCreator {

    public List<CycleValue> getCycleValuesList() {
        List<CycleValue> cycleValues = new ArrayList<>();
        for (String partition : getMainCycleValuesList()) {
            cycleValues.add(new CycleValue(partition, getNestedCycleValuesList()));
        }
        return cycleValues;
    }

    public List<String> getMainCycleValuesList() {

        return null;
    }

    public List<String> getNestedCycleValuesList() {

        return null;
    }
}
