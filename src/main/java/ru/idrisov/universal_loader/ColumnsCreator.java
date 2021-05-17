package ru.idrisov.universal_loader;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.springframework.stereotype.Component;
import ru.idrisov.universal_loader.annotations.*;
import ru.idrisov.universal_loader.entitys.TableSpark;
import ru.idrisov.universal_loader.enums.WherePlace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static ru.idrisov.universal_loader.utils.TableUtils.getColumnName;

@Component
@RequiredArgsConstructor
public class ColumnsCreator {

    final ColumnWithExpressionCreator columnWithExpressionCreator;

    public List<Column> getColumnsForWhere(TableSpark targetTable, WherePlace place) {

        List<Column> columnsForPreWhere = new ArrayList<>();
        getColumnsForWhereFromFields(targetTable, place, columnsForPreWhere);

        if (targetTable.getClass().isAnnotationPresent(WhereConditions.class)) {
            getColumnsForWhereFromClass(targetTable, place, columnsForPreWhere);
        }

        return columnsForPreWhere;
    }

    private void getColumnsForWhereFromFields(TableSpark targetTable, WherePlace place, List<Column> columnsForPreWhere) {
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
                        Column col = columnWithExpressionCreator.getColumnWithExpression(sourceTableInfo, whereCondition);
                        columnsForPreWhere.add(col);
                    });
                });
    }

    private void getColumnsForWhereFromClass(TableSpark targetTable, WherePlace place, List<Column> columnsForPreWhere) {
        Arrays.stream(targetTable.getClass().getAnnotation(WhereConditions.class).conditionsFields())
                .filter(conditionsFieldsInfo -> Arrays.stream(conditionsFieldsInfo.conditions())
                        .anyMatch(whereCondition -> whereCondition.place().equals(place)))
                .forEach(conditionsFieldsInfo -> {
                    WhereCondition[] whereConditions = conditionsFieldsInfo.conditions();

                    Arrays.stream(whereConditions)
                            .filter(whereCondition -> whereCondition.place().equals(place))
                            .forEach(whereCondition -> {
                                Column col = columnWithExpressionCreator.getColumnWithExpression(conditionsFieldsInfo, whereCondition);
                                columnsForPreWhere.add(col);
                            });
                });
    }

    public List<Column> getColumnsForJoin(Join join) {
        List<Column> columnsForPreWhere = new ArrayList<>();

        Arrays.stream(join.joinCondition())
                .forEach(joinCondition -> {

                    //TODO добавить возможность других проверок условий(если придумаю каких)
                    String mainColumnName = getColumnName(join.mainTable(), joinCondition.mainTableField());
                    String joinedColumnName = getColumnName(join.joinedTable(), joinCondition.joinedTableField());
                    Column col = expr(String.format(joinCondition.mainTableFunction(), mainColumnName))
                            .equalTo(expr(String.format(joinCondition.joinedTableFunction(), joinedColumnName)));

                    columnsForPreWhere.add(col);
                });
        return columnsForPreWhere;
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

    public Column getColumnFromColumnsList(List<Column> columnsList) {
        Column resultColumn = columnsList.remove(0);

        //TODO Реализовать поддержку or
        for (Column column : columnsList) {
            resultColumn = resultColumn.and(column);
        }

        return resultColumn;
    }
}
