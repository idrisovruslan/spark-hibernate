package ru.idrisov.universal_loader;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import ru.idrisov.SparkTestConfig;
import ru.idrisov.universal_loader.entitys.TableSpark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

@SpringBootTest(classes = SparkTestConfig.class)
@Slf4j
class CycleValuesHolderTest {

    @SpyBean
    CycleValuesCreator cycleValuesCreator;

    @Autowired
    CycleValuesHolder cycleValuesHolder;

    @Test
    void getAllCycleValues() {
        List<CycleValue> cycleValues = new ArrayList<>();
        cycleValues.add(createFirstMainCycleValue());
        doReturn(cycleValues).when(cycleValuesCreator).getCycleValuesList(any());

        cycleValuesHolder.init(TableSpark.class);
        List<Map<String, String>> one =  cycleValuesHolder.getAllCycleValues();

        assertEquals(9, one.size());

        for (Map<String, String> map : one) {
            for (String key : map.keySet()) {
                System.out.println(key + "   " + map.get(key));
            }
        }
    }

    CycleValue createFirstMainCycleValue() {
        //lvl1
        CycleValue firstMainCycleValue = new CycleValue("mainCycleValue", "firstMain");

        //lvl2
        CycleValue firstNestedCycleValue = new CycleValue("lvl1NestedCycleValue", "firstNested");
        CycleValue secondNestedCycleValue = new CycleValue("lvl1NestedCycleValue", "secondNested");
        CycleValue thirdNestedCycleValue = new CycleValue("lvl1NestedCycleValue", "thirdNested");

        //lvl3/1
        createLvl3CycleValue(firstNestedCycleValue, "firstFirstNested", "secondFirstNested", "thirdFirstNested");

        //lvl3/2
        createLvl3CycleValue(secondNestedCycleValue, "firstSecondNested", "secondSecondNested", "thirdSecondNested");

        //lvl3/3
        createLvl3CycleValue(thirdNestedCycleValue, "firstThirdNested", "secondThirdNested", "thirdThirdNested");


        List<CycleValue> mainNested = new ArrayList<>();
        mainNested.add(firstNestedCycleValue);
        mainNested.add(secondNestedCycleValue);
        mainNested.add(thirdNestedCycleValue);
        firstMainCycleValue.setNestedCycleValue(mainNested);

        return firstMainCycleValue;
    }

    private void createLvl3CycleValue(CycleValue firstNestedCycleValue, String firstFirstNestedValue, String secondFirstNestedValue, String thirdFirstNestedValue) {
        CycleValue firstFirstNestedCycleValue = new CycleValue("lvl2NestedCycleValue", firstFirstNestedValue);
        CycleValue secondFirstNestedCycleValue = new CycleValue("lvl2NestedCycleValue", secondFirstNestedValue);
        CycleValue thirdFirstNestedCycleValue = new CycleValue("lvl2NestedCycleValue", thirdFirstNestedValue);

        List<CycleValue> firstFirstNestedList = new ArrayList<>();
        firstFirstNestedList.add(firstFirstNestedCycleValue);
        firstFirstNestedList.add(secondFirstNestedCycleValue);
        firstFirstNestedList.add(thirdFirstNestedCycleValue);
        firstNestedCycleValue.setNestedCycleValue(firstFirstNestedList);
    }
}