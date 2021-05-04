package ru.idrisov.datamart;

import org.apache.spark.sql.Column;
import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.Join;
import ru.idrisov.domain.annotations.SourceTableField;
import ru.idrisov.domain.annotations.WhereCondition;
import ru.idrisov.domain.entitys.TargetTable;
import ru.idrisov.domain.enums.WherePlace;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static ru.idrisov.utils.TableUtils.getColumnName;

@Component
public class ColumnCreator {

    public List<Column> getColumnsForWhere(TargetTable targetTable, WherePlace place) {
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
                        //TODO Добавить проверку типа сравнения (equalTo,isin,isNull,leq,lt,geq,gt)
                        //TODO Добавить возможность сравнения с колонками(проверить поле type)

                        Column col = col(getColumnName(sourceTableInfo)).equalTo(whereCondition.value());
                        columnsForPreWhere.add(col);
                    });
                });

        return columnsForPreWhere;
    }

    public List<Column> getColumnsForJoin(Join join) {
        List<Column> columnsForPreWhere = new ArrayList<>();

        Arrays.stream(join.joinCondition())
                .forEach(joinCondition -> {
                    Column conditionColumn = col(getColumnName(join.mainTable(), joinCondition.mainTableField()))
                            .equalTo(col(getColumnName(join.joinedTable(), joinCondition.joinedTableField())));
                    columnsForPreWhere.add(conditionColumn);
                });
        return columnsForPreWhere;
    }

    public List<Column> getColumnsForSelect(TargetTable targetTable) {
        List<Column> listForSelect = new ArrayList<>();

        Arrays.stream(targetTable.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .forEach(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    String targetFieldName = field.getName();

                    Column col = col(getColumnName(sourceTableInfo)).as(targetFieldName);
                    listForSelect.add(col);
                });
        return listForSelect;
    }

    public Column getColumnFromColumnsList(List<Column> columnsList) {
        Column resultColumn = lit("1").equalTo("1");

        if(columnsList.isEmpty()){
            return resultColumn;
        }
        //TODO Реализовать поддержку or
        for (Column column : columnsList) {
            resultColumn = resultColumn.and(column);
        }

        return resultColumn;
    }
}
