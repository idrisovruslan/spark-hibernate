package ru.idrisov.universal_loader;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.springframework.stereotype.Component;
import ru.idrisov.universal_loader.annotations.*;
import ru.idrisov.universal_loader.entitys.TableSpark;
import ru.idrisov.universal_loader.enums.WherePlace;

import java.util.*;

import static org.apache.spark.sql.functions.*;
import static ru.idrisov.universal_loader.utils.TableUtils.getColumnName;

@Component
@RequiredArgsConstructor
public class ColumnsCreator {

    final ColumnWithExpressionCreator columnWithExpressionCreator;

    public Map<Integer, List<Column>> getColumnsForWhereCondition(TableSpark targetTable, WherePlace place) {

        Map<Integer, List<Column>> columnsForWhereWithOrGroup = new HashMap<>();

        getColumnsForWhereFromFields(targetTable, place, columnsForWhereWithOrGroup);

        if (targetTable.getClass().isAnnotationPresent(WhereConditions.class)) {
            getColumnsForWhereFromClass(targetTable, place, columnsForWhereWithOrGroup);
        }

        return columnsForWhereWithOrGroup;
    }

    private void getColumnsForWhereFromFields(TableSpark targetTable, WherePlace place, Map<Integer, List<Column>> columnsForWhereWithOrGroup) {
        Arrays.stream(targetTable.getClass().getFields())
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .filter(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    return Arrays.stream(sourceTableInfo.conditions())
                            .anyMatch(whereCondition -> whereCondition.place().equals(place));
                })
                .forEach(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    WhereCondition[] whereConditions = sourceTableInfo.conditions();

                    Arrays.stream(whereConditions)
                            .filter(whereCondition -> whereCondition.place().equals(place))
                            .forEach(whereCondition -> {
                                if (columnsForWhereWithOrGroup.containsKey(whereCondition.orGroup())) {
                                    Column col = columnWithExpressionCreator.getColumnWithExpression(sourceTableInfo, whereCondition);
                                    columnsForWhereWithOrGroup.get(whereCondition.orGroup()).add(col);
                                } else {
                                    List<Column> columnsForWhere = new ArrayList<>();
                                    Column col = columnWithExpressionCreator.getColumnWithExpression(sourceTableInfo, whereCondition);
                                    columnsForWhere.add(col);
                                    columnsForWhereWithOrGroup.put(whereCondition.orGroup(), columnsForWhere);
                                }
                    });
                });
    }

    private void getColumnsForWhereFromClass(TableSpark targetTable, WherePlace place, Map<Integer, List<Column>> columnsForWhereWithOrGroup) {
        Arrays.stream(targetTable.getClass().getAnnotation(WhereConditions.class).conditionsFields())
                .filter(conditionsFieldsInfo -> Arrays.stream(conditionsFieldsInfo.conditions())
                        .anyMatch(whereCondition -> whereCondition.place().equals(place)))
                .forEach(conditionsFieldsInfo -> {
                    WhereCondition[] whereConditions = conditionsFieldsInfo.conditions();

                    Arrays.stream(whereConditions)
                            .filter(whereCondition -> whereCondition.place().equals(place))
                            .forEach(whereCondition -> {
                                if (columnsForWhereWithOrGroup.containsKey(whereCondition.orGroup())) {
                                    Column col = columnWithExpressionCreator.getColumnWithExpression(conditionsFieldsInfo, whereCondition);
                                    columnsForWhereWithOrGroup.get(whereCondition.orGroup()).add(col);
                                } else {
                                    List<Column> columnsForWhere = new ArrayList<>();
                                    Column col = columnWithExpressionCreator.getColumnWithExpression(conditionsFieldsInfo, whereCondition);
                                    columnsForWhere.add(col);
                                    columnsForWhereWithOrGroup.put(whereCondition.orGroup(), columnsForWhere);
                                }
                            });
                });
    }

    public Map<Integer, List<Column>> getColumnsForJoinCondition(Join join) {
        Map<Integer, List<Column>> columnsForJoinWithOrGroup = new HashMap<>();
        Arrays.stream(join.joinCondition())
                .forEach(joinCondition -> {

                    if (columnsForJoinWithOrGroup.containsKey(joinCondition.orGroup())) {
                        Column col = createExpresionColumn(join, joinCondition);
                        columnsForJoinWithOrGroup.get(joinCondition.orGroup()).add(col);
                    } else {
                        List<Column> columnsForJoin = new ArrayList<>();
                        Column col = createExpresionColumn(join, joinCondition);
                        columnsForJoin.add(col);
                        columnsForJoinWithOrGroup.put(joinCondition.orGroup(), columnsForJoin);
                    }

                });
        return columnsForJoinWithOrGroup;
    }

    private Column createExpresionColumn(Join join, JoinCondition joinCondition) {
        String mainColumnName = getColumnName(join.mainTable(), joinCondition.mainTableField());
        String joinedColumnName = getColumnName(join.joinedTable(), joinCondition.joinedTableField());
        return expr(String.format(joinCondition.mainTableFunction(), mainColumnName))
                .equalTo(expr(String.format(joinCondition.joinedTableFunction(), joinedColumnName)));
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

    public Column getConditionColumnFromColumnsList(Map<Integer, List<Column>> columnsWithOrGroup) {
        Column resultColumn;

        if (orGroupIsEmpty(columnsWithOrGroup)) {
            List<Column> columnsList = columnsWithOrGroup.get(-1);
            resultColumn = columnsList.remove(0);
            for (Column column : columnsList) {
                resultColumn = resultColumn.and(column);
            }
        } else {
            resultColumn = lit("1").equalTo("1");
            for (int key : columnsWithOrGroup.keySet()) {
                List<Column> columnsList = columnsWithOrGroup.get(key);
                Column tempColumn = columnsList.remove(0);

                for (Column column : columnsList) {
                    tempColumn = tempColumn.or(column);
                }
                resultColumn = resultColumn.and(tempColumn);
            }
        }

        return resultColumn;
    }

    private boolean orGroupIsEmpty(Map<Integer, List<Column>> columnsWithOrGroup) {
        return columnsWithOrGroup.size() == 1 && columnsWithOrGroup.containsKey(-1);
    }
}
