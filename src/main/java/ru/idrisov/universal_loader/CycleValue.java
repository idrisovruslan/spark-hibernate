package ru.idrisov.universal_loader;

import lombok.Getter;
import lombok.Setter;

import java.util.List;


public class CycleValue {
    @Getter
    private final String mainCycleName;

    @Getter
    private final String mainCycleValue;

    @Setter
    private List<CycleValue> nestedCycleValue;

    @Getter
    private Boolean processed = false;

    public CycleValue(String mainCycleName, String mainCycleValue) {
        this.mainCycleName = mainCycleName;
        this.mainCycleValue = mainCycleValue;
        nestedCycleValue = null;
    }

    public CycleValue getFirstNestedCycleValueAndRemoveIfLast() {
        removeNestedCycleValueIfProcessed();
        setProcessedFlag();

        if (processed) {
            return null;
        }

        if (nestedCycleValue.get(0).nestedCycleIsPresent()) {
            return nestedCycleValue.get(0);
        }
        CycleValue result = nestedCycleValue.remove(0);
        setProcessedFlag();
        return result;
    }

    public Boolean nestedCycleIsPresent() {
        return nestedCycleValue != null;// && nestedCycleValue.size() != 0;
    }

    public void setProcessedFlag() {
        if (nestedCycleValue.size() == 0) {
            processed = true;
        }
    }

    public void removeNestedCycleValueIfProcessed() {
        if (nestedCycleValue.get(0).processed) {
            nestedCycleValue.remove(0);
        }
    }
}
