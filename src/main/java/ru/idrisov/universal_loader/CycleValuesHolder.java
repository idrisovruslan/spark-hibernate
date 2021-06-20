package ru.idrisov.universal_loader;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.idrisov.universal_loader.exceptions.AllNestedCycleProcessedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class CycleValuesHolder {

    private List<CycleValue> cycleValues;

    private Map<String, String> currentCycleValue;

    //На момент работы CycleDfCreator currentCycleValue = null т.к. CycleValuesHolder
    // не проиницилизирован, но уже используеться.
    //TODO Придумай получше.
    private Map<String, String> tempCycleValueForCycleDfCreator;

    //For tests
    @Getter(value = AccessLevel.PACKAGE)
    private List<Map<String, String>> allCycleValues;

    public void init(List<CycleValue> cycleValues) {
        if (cycleValues == null) {
            return;
        }

        this.cycleValues = cycleValues;

        allCycleValues = fillListWithAllCycleValues();
        setNextCurrentCycleValue();
    }

    public void setNextCurrentCycleValue() {
        if (allCycleValues.size() != 0) {
            currentCycleValue = allCycleValues.remove(0);
        }
    }

    public boolean nextValuesIsPresent() {
        return allCycleValues.size() > 0;
    }

    public String getValue(String key) {
        if (currentCycleValue.size() != 0) {
            return currentCycleValue.get(key);
        }
        return tempCycleValueForCycleDfCreator.get(key);
    }

    public boolean currentCycleValueIsPresent() {
        return currentCycleValue != null;
    }

    //TODO Придумай нормальное название БЛЕАТЬ!
    public void put2TempCycleValueForCycleDfCreator(String cycleName, String cycleValue) {
        tempCycleValueForCycleDfCreator.put(cycleName, cycleValue);
    }

    List<Map<String, String>> fillListWithAllCycleValues() {
        List<Map<String, String>> result = new ArrayList<>();

        //TODO напиши нормально БЛЕАТЬ!
        for (CycleValue cycleValue : cycleValues) {
            while (true) {
                try {
                    result.add(getNextValuesFromCycleValue(cycleValue));
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
    private Map<String, String> getNextValuesFromCycleValue(CycleValue mainCycleValue) throws AllNestedCycleProcessedException {
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
