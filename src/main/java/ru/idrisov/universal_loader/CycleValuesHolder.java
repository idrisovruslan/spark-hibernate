package ru.idrisov.universal_loader;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.idrisov.universal_loader.entitys.TableSpark;
import ru.idrisov.universal_loader.exceptions.AllNestedCycleProcessedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class CycleValuesHolder {

    private final CycleValuesCreator cycleValuesCreator;

    private List<CycleValue> cycleValues;

    @Getter
    private List<Map<String, String>> allCycleValues;


    public void init(Class<? extends TableSpark> tableInfo) {
        cycleValues = cycleValuesCreator.getCycleValuesList(tableInfo);
        allCycleValues = fillListWithAllCycleValues();
    }

    List<Map<String, String>> fillListWithAllCycleValues() {
        List<Map<String, String>> result = new ArrayList<>();

        //TODO напиши нормально БЛЕАТЬ!
        for (CycleValue cycleValue : cycleValues) {
            while (true) {
                try {
                    result.add(getNextValues(cycleValue));
                } catch (AllNestedCycleProcessedException e) {
                    break;
                }
            }
        }

        return result;
    }

    //TODO заменить <String, String> на объект(пока хз как, из за то го что по сути будет
    // дублироваться объект CycleValue только без ссылок, как вариант
    // создать объект и использовать его в CycleValue)
    private Map<String, String> getNextValues(CycleValue mainCycleValue) throws AllNestedCycleProcessedException {
        Map<String, String> result = new HashMap<>();
        result.put(mainCycleValue.getMainCycleName(), mainCycleValue.getMainCycleValue());
        CycleValue cycleValue = mainCycleValue;

        while (cycleValue.nestedCycleIsPresent()) {
            cycleValue = cycleValue.getFirstNotProcessedNestedCycleValue();
            //Так как обратная ссылка в иерархии не храниться, мы не можем вернуться
            //назад по иерархии если значение обработанно, поэтому возвращаемся
            //в начало иерархии после установки флага обработки
            if (cycleValue == null) {
                if (mainCycleValue.allNestedCycleProcessed()) {
                    throw new AllNestedCycleProcessedException("Всё прогнали");
                }
                cycleValue = mainCycleValue;
                continue;
            }

            result.put(cycleValue.getMainCycleName(), cycleValue.getMainCycleValue());
        }

        return result;
    }

}
