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

    @Setter
    private Boolean processed = false;

    public CycleValue(String mainCycleName, String mainCycleValue) {
        this.mainCycleName = mainCycleName;
        this.mainCycleValue = mainCycleValue;
        nestedCycleValue = null;
    }

    public CycleValue getFirstNotProcessedNestedCycleValue() {
        checkAndSetProcessedFlag();

        if (processed) {
            return null;
        }

        CycleValue resultCycleValue = nestedCycleValue.stream().filter(x -> !x.processed).findFirst().get();
        if (!resultCycleValue.nestedCycleIsPresent()) {
            resultCycleValue.setProcessed(true);
        }

         return resultCycleValue;
    }

    public Boolean nestedCycleIsPresent() {
        return nestedCycleValue != null;
    }

    public void checkAndSetProcessedFlag() {
        if (allNestedCycleProcessed()) {
            processed = true;
        }
    }

    public boolean allNestedCycleProcessed() {
        return nestedCycleValue.stream().allMatch(x -> x.processed);
    }

}
