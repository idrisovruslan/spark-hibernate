package ru.idrisov.datamart.universal_loader;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.*;
import ru.idrisov.domain.entitys.TableSpark;
import ru.idrisov.domain.enums.WherePlace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static ru.idrisov.utils.TableUtils.getColumnName;

@Component
@RequiredArgsConstructor
public class ColumnCreator {

    final ColumnWithExpressionCreator columnWithExpressionCreator;
    final ColumnWithAggFunctionCreator columnWithAggFunctionCreator;

    public List<Column> getColumnsForWhere(TableSpark targetTable, WherePlace place) {
        List<Column> columnsForPreWhere = new ArrayList<>();

        Arrays.stream(targetTable.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .filter(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    return Arrays.stream(sourceTableInfo.conditions())
                            .anyMatch(whereCondition -> whereCondition.place().equals(place));
                })
                .forEach(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    WhereCondition[] whereConditions = sourceTableInfo.conditions();

                    Arrays.stream(whereConditions).forEach(whereCondition -> {
                        Column col = columnWithExpressionCreator.getColumnWithExpression(sourceTableInfo, whereCondition);
                        columnsForPreWhere.add(col);
                    });
                });

        return columnsForPreWhere;
    }

    public List<Column> getColumnsForJoin(Join join) {
        List<Column> columnsForPreWhere = new ArrayList<>();

        Arrays.stream(join.joinCondition())
                .forEach(joinCondition -> {
                    //TODO добавить возможность других проверок условий(если придумаю каких)
                    Column conditionColumn = col(getColumnName(join.mainTable(), joinCondition.mainTableField()))
                            .equalTo(col(getColumnName(join.joinedTable(), joinCondition.joinedTableField())));
                    columnsForPreWhere.add(conditionColumn);
                });
        return columnsForPreWhere;
    }

    public List<Column> getColumnsForGroupBy(TableSpark targetTable) {
        List<Column> listForGroupBy = new ArrayList<>();

        Arrays.stream(targetTable.getClass().getDeclaredFields())
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

        Arrays.stream(targetTable.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Aggregate.class))
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .forEach(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    Aggregate aggregateInfo = field.getAnnotation(Aggregate.class);

                    //TODO не доделан
                    Column col = columnWithAggFunctionCreator.getColumnWithAggFunction(aggregateInfo, sourceTableInfo);

                    listForSelect.add(col);
                });
        return listForSelect;
    }

    public List<Column> getColumnsForSelect(TableSpark targetTable, Boolean aggregated) {
        List<Column> listForSelect = new ArrayList<>();

        Arrays.stream(targetTable.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .forEach(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    String targetFieldName = field.getName();

                    StringBuilder columnName = new StringBuilder(getColumnName(sourceTableInfo));
                    if (aggregated) {
                        columnName.insert(0, "`").append("`");
                    }

                    Column col = col(columnName.toString()).as(targetFieldName);

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
