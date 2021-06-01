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

    private Map<String, String> getNextValues(CycleValue mainCycleValue) {
        Map<String, String> result = new HashMap<>();
        result.put(mainCycleValue.getMainCycleName(), mainCycleValue.getMainCycleValue());
        CycleValue cycleValue = mainCycleValue;

        while (mainCycleValue.nestedCycleIsPresent()) {
            cycleValue = cycleValue.getFirstNestedCycleValueAndRemoveIfLast();
            //Так как обратная ссылка в иерархии не храниться, мы не можем вернуться
            //назад по иерархии если значение обработанно, поэтому возвращаемся
            //в начало иерархии после установки флага обработки
            if (cycleValue == null) {
                cycleValue = mainCycleValue;
                continue;
            }

            result.put(cycleValue.getMainCycleName(), cycleValue.getMainCycleValue());
        }

        return result;
    }

}
