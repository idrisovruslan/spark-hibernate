package ru.idrisov.universal_loader;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.springframework.stereotype.Component;
import ru.idrisov.universal_loader.annotations.*;
import ru.idrisov.universal_loader.entitys.TableSpark;
import ru.idrisov.universal_loader.enums.WherePlace;

import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static ru.idrisov.universal_loader.utils.TableUtils.getColumnName;

@Component
@RequiredArgsConstructor
public class ColumnsCreator {

    final ColumnWithExpressionCreator columnWithExpressionCreator;

    public Map<Integer, List<Column>> getColumnsForWhereCondition(TableSpark targetTable, WherePlace place) {

        Map<Integer, List<Column>> columnsForWhereWithOrGroup = new HashMap<>();
        getColumnsForWhereConditionFromFields(targetTable, place, columnsForWhereWithOrGroup);

        if (targetTable.getClass().isAnnotationPresent(WhereConditions.class)) {
            getColumnsForWhereConditionFromClass(targetTable, place, columnsForWhereWithOrGroup);
        }

        return columnsForWhereWithOrGroup;
    }

    private void getColumnsForWhereConditionFromFields(TableSpark targetTable, WherePlace place, Map<Integer, List<Column>> columnsForWhereWithOrGroup) {
        Arrays.stream(targetTable.getClass().getFields())
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .filter(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    return Arrays.stream(sourceTableInfo.conditions())
                            .anyMatch(whereCondition -> whereCondition.place().equals(place));
                })
                .forEach(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    fillMapWithOrGroup(place, columnsForWhereWithOrGroup, sourceTableInfo);
                });
    }

    private void getColumnsForWhereConditionFromClass(TableSpark targetTable, WherePlace place, Map<Integer, List<Column>> columnsForWhereWithOrGroup) {
        Arrays.stream(targetTable.getClass().getAnnotation(WhereConditions.class).conditionsFields())
                .filter(conditionsFieldsInfo -> Arrays.stream(conditionsFieldsInfo.conditions())
                        .anyMatch(whereCondition -> whereCondition.place().equals(place)))
                .forEach(conditionsFieldsInfo -> {
                    fillMapWithOrGroup(place, columnsForWhereWithOrGroup, conditionsFieldsInfo);
                });
    }

    private void fillMapWithOrGroup(WherePlace place, Map<Integer, List<Column>> columnsForWhereWithOrGroup, SourceTableField sourceTableInfo) {
        WhereCondition[] whereConditions = sourceTableInfo.conditions();

        Arrays.stream(whereConditions)
                .filter(whereCondition -> whereCondition.place().equals(place))
                .forEach(whereCondition -> {
                    Column col = columnWithExpressionCreator.getColumnWithExpressionFromWhereCondition(sourceTableInfo, whereCondition);
                    fillMapWithConcreteOrGroup(columnsForWhereWithOrGroup, col, whereCondition.orGroup());
                });
    }

    public Map<Integer, List<Column>> getColumnsForJoinCondition(Join join) {
        Map<Integer, List<Column>> columnsForJoinWithOrGroup = new HashMap<>();
        Arrays.stream(join.joinCondition())
                .forEach(joinCondition -> {
                    Column col = columnWithExpressionCreator.getColumnWithExpressionFromJoinCondition(join, joinCondition);
                    fillMapWithConcreteOrGroup(columnsForJoinWithOrGroup, col, joinCondition.orGroup());
                });
        return columnsForJoinWithOrGroup;
    }

    private void fillMapWithConcreteOrGroup(Map<Integer, List<Column>> columnsForJoinWithOrGroup, Column col, int orGroup) {
        if (columnsForJoinWithOrGroup.containsKey(orGroup)) {
            columnsForJoinWithOrGroup.get(orGroup).add(col);
        } else {
            List<Column> columnsForJoin = new ArrayList<>();
            columnsForJoin.add(col);
            columnsForJoinWithOrGroup.put(orGroup, columnsForJoin);
        }
    }

    public List<Column> getColumnsForGroupBy(TableSpark targetTable) {
        List<Column> listForGroupBy = new ArrayList<>();

        Arrays.stream(targetTable.getClass().getFields())
                .filter(field -> field.isAnnotationPresent(GroupBy.class))
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .forEach(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    Column col = col(getColumnName(sourceTableInfo)).as(getColumnName(sourceTableInfo));

                    listForGroupBy.add(col);
                });
        return listForGroupBy;
    }

    public List<Column> getColumnsForAgg(TableSpark targetTable) {
        List<Column> listForSelect = new ArrayList<>();

        Arrays.stream(targetTable.getClass().getFields())
                .filter(field -> field.isAnnotationPresent(Aggregate.class))
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .forEach(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    Aggregate aggregateInfo = field.getAnnotation(Aggregate.class);

                    String columnName = getColumnName(sourceTableInfo);
                    Column col = expr(String.format(aggregateInfo.function(), columnName)).as(columnName);

                    listForSelect.add(col);
                });
        return listForSelect;
    }

    public List<Column> getColumnsForSelect(TableSpark targetTable, Boolean aggregated) {
        List<Column> listForSelect = new ArrayList<>();

        Arrays.stream(targetTable.getClass().getFields())
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .forEach(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    String targetFieldName = field.getName();

                    StringBuilder columnName = new StringBuilder(getColumnName(sourceTableInfo));
                    if (aggregated) {
                        columnName.insert(0, "`").append("`");
                    }

                    Column col = expr(String.format(sourceTableInfo.function(), columnName)).as(targetFieldName);

                    listForSelect.add(col);
                });
        return listForSelect;
    }

    public Column getConditionColumnFromColumnsMap(Map<Integer, List<Column>> columnsWithOrGroup) {
        Column resultColumn;

        if (orGroupIsEmpty(columnsWithOrGroup)) {
            resultColumn = createConditionColumnWithoutOrGroup(columnsWithOrGroup);
        } else {
            resultColumn = createConditionColumnWithOrGroup(columnsWithOrGroup);
        }

        return resultColumn;
    }

    private Column createConditionColumnWithoutOrGroup(Map<Integer, List<Column>> columnsWithOrGroup) {
        Column resultColumn;
        List<Column> columnsList = columnsWithOrGroup.get(-1);
        resultColumn = columnsList.remove(0);
        for (Column column : columnsList) {
            resultColumn = resultColumn.and(column);
        }
        return resultColumn;
    }

    private Column createConditionColumnWithOrGroup(Map<Integer, List<Column>> columnsWithOrGroup) {
        Column resultColumn = null;
        for (int key : columnsWithOrGroup.keySet()) {
            List<Column> columnsList = columnsWithOrGroup.get(key);
            Column tempColumn = columnsList.remove(0);

            for (Column column : columnsList) {
                tempColumn = tempColumn.or(column);
            }

            if (resultColumn == null) {
                resultColumn = tempColumn;
                continue;
            }

            resultColumn = resultColumn.and(tempColumn);
        }
        return resultColumn;
    }

    private boolean orGroupIsEmpty(Map<Integer, List<Column>> columnsWithOrGroup) {
        return columnsWithOrGroup.size() == 1 && columnsWithOrGroup.containsKey(-1);
    }
}
