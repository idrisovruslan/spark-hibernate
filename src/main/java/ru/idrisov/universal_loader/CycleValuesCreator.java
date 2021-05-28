package ru.idrisov.universal_loader;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;
import ru.idrisov.universal_loader.annotations.Cycle;
import ru.idrisov.universal_loader.annotations.Cycles;
import ru.idrisov.universal_loader.entitys.TableSpark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static ru.idrisov.universal_loader.utils.TableUtils.getColumnName;
import static ru.idrisov.universal_loader.utils.TableUtils.mapToList;

@Service
@RequiredArgsConstructor
public class CycleValuesCreator {

    CycleDfCreator cycleDfCreator;

    public List<CycleValue> getCycleValuesList(Class<? extends TableSpark> tableInfo) {

        if (!tableInfo.isAnnotationPresent(Cycles.class)) {
            return null;
        }

        Cycle cycle = getMainCycle(tableInfo);
        return getCycleValuesListWithRecursion(tableInfo, cycle);
    }

    private Cycle getMainCycle(Class<? extends TableSpark> tableInfo) {
        String mainCycleName = getMainCycleName(tableInfo);
        return getCycleFromName(tableInfo, mainCycleName);
    }

    private String getMainCycleName(Class<? extends TableSpark> tableInfo) {
        List<String> cycleNames = new ArrayList<>();
        List<String> nestedCycleNames = new ArrayList<>();

        Arrays.stream(getCycles(tableInfo).value()).forEach(cycle -> {
            cycleNames.add(cycle.cycleName());
            nestedCycleNames.add(cycle.nestedCycleName());
        });

        cycleNames.removeAll(nestedCycleNames);

        if (cycleNames.size() == 1) {
            return cycleNames.get(0);
        }

        throw new RuntimeException("Не верно заполнен цикл");
    }

    private Cycle getCycleFromName(Class<? extends TableSpark> tableInfo, String mainCycleName) {
        List<Cycle> resultList =  Arrays.stream(getCycles(tableInfo).value())
                .filter(cycle -> cycle.cycleName().equals(mainCycleName))
                .collect(Collectors.toList());

        if (resultList.size() == 1) {
            return resultList.get(0);
        }
        throw new RuntimeException("Не верно заполнен цикл");
    }

    private List<CycleValue> getCycleValuesListWithRecursion(Class<? extends TableSpark> tableInfo, Cycle cycle) {

        List<CycleValue> cycleValues = new ArrayList<>();

        Dataset<Row> cycleValuesDf = cycleDfCreator.getCycleDf(cycle);
        List<String> cycleValuesList = mapToList(cycleValuesDf, getColumnName(tableInfo, cycle.sourceTableField().sourceFieldName()));

        String cycleName = cycle.cycleName();

        for (String cycleValueString : cycleValuesList) {
            CycleValue cycleValue = new CycleValue(cycleName, cycleValueString);

            if (!cycle.nestedCycleName().equals("")) {
                List<CycleValue> nestedCycleValues = getCycleValuesListWithRecursion(tableInfo, getNestedCycle(tableInfo, cycle));
                cycleValue.setNestedCycleValue(nestedCycleValues);
            }

            cycleValues.add(cycleValue);
        }
        return cycleValues;
    }

    private Cycle getNestedCycle(Class<? extends TableSpark> tableInfo, Cycle mainCycle) {
        String nestedCycleName = mainCycle.nestedCycleName();
        return getCycleFromName(tableInfo, nestedCycleName);
    }

    private Cycles getCycles(Class<? extends TableSpark> tableInfo) {
        return tableInfo.getAnnotation(Cycles.class);
    }
}
