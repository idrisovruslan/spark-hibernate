package ru.idrisov.universal_loader;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import ru.idrisov.SparkTestConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

        cycleValuesHolder.init(cycleValues);
        List<Map<String, String>> one =  cycleValuesHolder.getAllCycleValues();

        assertEquals(8, one.size());

        assertEquals("firstMain", cycleValuesHolder.getValue("mainCycleValue"));
        assertEquals("firstNested", cycleValuesHolder.getValue("lvl1NestedCycleValue"));
        assertEquals("firstFirstNested", cycleValuesHolder.getValue("lvl2NestedCycleValue"));

        assertEquals("firstMain", one.get(2).get("mainCycleValue"));
        assertEquals("secondNested", one.get(2).get("lvl1NestedCycleValue"));
        assertEquals("firstSecondNested", one.get(2).get("lvl2NestedCycleValue"));

        assertEquals("firstMain", one.get(7).get("mainCycleValue"));
        assertEquals("thirdNested", one.get(7).get("lvl1NestedCycleValue"));
        assertEquals("thirdThirdNested", one.get(7).get("lvl2NestedCycleValue"));

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