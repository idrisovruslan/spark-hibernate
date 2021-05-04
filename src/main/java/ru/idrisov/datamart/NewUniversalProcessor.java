package ru.idrisov.datamart;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import ru.idrisov.domain.annotations.*;
import ru.idrisov.domain.entitys.TableSpark;
import ru.idrisov.domain.entitys.TargetTable;
import ru.idrisov.domain.enums.WherePlace;

import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static ru.idrisov.utils.TableUtils.*;

@Service
@AllArgsConstructor
public class NewUniversalProcessor {

    ApplicationContext applicationContext;
    SparkSession sparkSession;

    public void fillTable(TargetTable targetTable) {

        Map<String, Dataset<Row>> sourceDfs = getSourceDfsMap(targetTable);

        Dataset<Row> mainSourceDf = getMainSourceDf(targetTable, sourceDfs);

        Dataset<Row> targetDf = getTargetDfWithWhereBeforeJoin(targetTable, mainSourceDf);

        targetDf = getTargetDfWithJoins(targetTable, sourceDfs, targetDf);

        targetDf = getTargetDfWithWhereAfterJoin(targetTable, targetDf);

        targetDf = getResultTargetDf(targetTable, targetDf);

        saveAsTable(targetDf, targetTable);
    }

    private Map<String, Dataset<Row>> getSourceDfsMap(TargetTable targetTable) {
        Map<String, Dataset<Row>> sourcesDfs = new HashMap<>();
        getSourceTables(targetTable).forEach(tableSparkClass -> {
            String tableAliasName = getTableAliasName(tableSparkClass);
            Dataset<Row> sourceDf = readTable(sparkSession, applicationContext.getBean(tableSparkClass)).alias(tableAliasName);
            sourcesDfs.put(tableAliasName, sourceDf);
        });
        return sourcesDfs;
    }

    private Set<Class<? extends TableSpark>> getSourceTables(TargetTable targetTable) {
        Set<Class<? extends TableSpark>> set = new HashSet<>();
        Arrays.stream(targetTable.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .forEach(field -> set.add(field.getAnnotation(SourceTableField.class).sourceTable()));
        return set;
    }

    private Dataset<Row> getTargetDfWithWhereBeforeJoin(TargetTable targetTable, Dataset<Row> currentDf) {
        return getTargetDfWithWhere(targetTable, currentDf, WherePlace.BEFORE_JOIN);
    }

    private Dataset<Row> getTargetDfWithWhereAfterJoin(TargetTable targetTable, Dataset<Row> currentDf) {
        return getTargetDfWithWhere(targetTable, currentDf, WherePlace.AFTER_JOIN);
    }

    private Dataset<Row> getTargetDfWithWhere(TargetTable targetTable, Dataset<Row> currentDf, WherePlace place) {
        List<Column> columnsForWhereBeforeJoin = getColumnsForWhere(targetTable, place);
        Column columnForPreWhere = getColumnFromColumnsList(columnsForWhereBeforeJoin);

        currentDf = currentDf
                .where(
                        columnForPreWhere
                );
        return currentDf;
    }

    private Dataset<Row> getTargetDfWithJoins(TargetTable targetTable, Map<String, Dataset<Row>> sourceDfs, Dataset<Row> currentDf) {
        for (Join join : targetTable.getClass().getAnnotation(Joins.class).joins()) {

            List<Column> columnsForJoin = getColumnsForJoin(join);
            Column columnForJoin = getColumnFromColumnsList(columnsForJoin);

            currentDf = currentDf
                    .join(sourceDfs.get(getTableAliasName(join.joinedTable())),
                            columnForJoin,
                            join.joinType().getJoinType()
                    );
        }

        return currentDf;
    }

    private List<Column> getColumnsForJoin(Join join) {
        List<Column> columnsForPreWhere = new ArrayList<>();

        Arrays.stream(join.joinCondition())
                .forEach(joinCondition -> {
                    Column conditionColumn = col(getColumnName(join.mainTable(), joinCondition.mainTableField()))
                            .equalTo(col(getColumnName(join.joinedTable(), joinCondition.joinedTableField())));
                    columnsForPreWhere.add(conditionColumn);
                });
        return columnsForPreWhere;
    }

    private Dataset<Row> getResultTargetDf(TargetTable targetTable, Dataset<Row> currentDf) {
        List<Column> columnsForSelect = getColumnsForSelect(targetTable);
        currentDf = currentDf
                .select(
                        columnsForSelect.toArray(new Column[0])
                );
        return currentDf;
    }

    private List<Column> getColumnsForWhere(TargetTable targetTable, WherePlace place) {
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

    private Column getColumnFromColumnsList(List<Column> columnsList) {
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

    private List<Column> getColumnsForSelect(TargetTable targetTable) {
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

    private String getColumnName(SourceTableField sourceTableInfo) {
        String sourceTableName = getTableAliasName(sourceTableInfo.sourceTable());
        String sourceFieldName = sourceTableInfo.sourceFieldName();
        return sourceTableName + "." + sourceFieldName;
    }

    private String getColumnName(Class<? extends TableSpark> tableSpark, String fieldName) {
        String sourceTableName = getTableAliasName(tableSpark);
        return sourceTableName + "." + fieldName;
    }

    private Class<? extends TableSpark> getMainSourceTableClass(TargetTable targetTable) {

        Set<Class<? extends TableSpark>> sourceTables = getSourceTables(targetTable);

        Class<? extends TableSpark> mainSourceTable = sourceTables.iterator().next();

        if (sourceTables.size() > 1) {
            mainSourceTable = targetTable.getClass().getAnnotation(Joins.class).joins()[0].mainTable();
        }

        return mainSourceTable;
    }

    private Dataset<Row> getMainSourceDf(TargetTable targetTable, Map<String, Dataset<Row>> sourceDfs) {
        Dataset<Row> sourceDf = sourceDfs.get(sourceDfs.keySet().iterator().next());

        if (sourceDfs.keySet().size() > 1) {
            Join[] joins = targetTable.getClass().getAnnotation(Joins.class).joins();
            sourceDf = sourceDfs.get(getTableAliasName(joins[0].mainTable()));
        }
        return sourceDf;
    }
}
