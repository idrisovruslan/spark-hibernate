package ru.idrisov.universal_loader;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;
import ru.idrisov.universal_loader.annotations.Cycle;
import ru.idrisov.universal_loader.annotations.Cycles;
import ru.idrisov.universal_loader.entitys.TableSpark;
import ru.idrisov.universal_loader.exceptions.EmptyCycleValuesListException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static ru.idrisov.universal_loader.utils.TableUtils.getColumnName;
import static ru.idrisov.universal_loader.utils.TableUtils.mapToList;

@Service
@RequiredArgsConstructor
public class CycleValuesCreator {

    private final CycleDfCreator cycleDfCreator;
    private final CycleValuesHolder cycleValuesHolder;

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

        Arrays.stream(getCyclesAnnotation(tableInfo).value()).forEach(cycle -> {
            cycleNames.add(cycle.cycleName());
            nestedCycleNames.add(cycle.nestedCycleName());
        });

        //После удаления останеться только самый верхний(главный) цикл
        cycleNames.removeAll(nestedCycleNames);

        if (cycleNames.size() == 1) {
            return cycleNames.get(0);
        }

        throw new RuntimeException("Не верно заполнен цикл");
    }

    private Cycle getCycleFromName(Class<? extends TableSpark> tableInfo, String mainCycleName) {
        List<Cycle> resultList =  Arrays.stream(getCyclesAnnotation(tableInfo).value())
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
        String columnName = "`" + getColumnName(cycle.sourceTableField().sourceTable(), cycle.sourceTableField().sourceFieldName()) + "`";
        List<String> cycleValuesList = mapToList(cycleValuesDf, columnName);

        if (cycleValuesList.size() == 0 ) {
            throw new EmptyCycleValuesListException("Нет ни одного значения");
        }

        String cycleName = cycle.cycleName();

        for (String cycleValueString : cycleValuesList) {
            CycleValue cycleValue = new CycleValue(cycleName, cycleValueString);

            if (!cycle.nestedCycleName().equals("")) {
                cycleValuesHolder.put2TempCycleValueForCycleDfCreator(cycleName, cycleValueString);
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

    private Cycles getCyclesAnnotation(Class<? extends TableSpark> tableInfo) {
        return tableInfo.getAnnotation(Cycles.class);
    }
}
