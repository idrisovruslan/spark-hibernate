package ru.idrisov.datamart;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import ru.idrisov.domain.annotations.WhereCondition;
import ru.idrisov.domain.annotations.Join;
import ru.idrisov.domain.annotations.Joins;
import ru.idrisov.domain.annotations.SourceTableField;
import ru.idrisov.domain.entitys.TableSpark;
import ru.idrisov.domain.entitys.TargetTable;
import ru.idrisov.domain.enums.WherePlaces;

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
        mainSourceDf.show();

        Dataset<Row> targetDf = getTargetDfWithWhereBeforeJoin(targetTable, mainSourceDf);

        targetDf = getTargetDfWithJoins(targetTable, sourceDfs, targetDf);

        targetDf = getTargetDfWithWhereAfterJoin(targetTable, targetDf);

        targetDf = getResultTargetDf(targetTable, targetDf);

        targetDf.show();
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
        return getTargetDfWithWhere(targetTable, currentDf, WherePlaces.BEFORE);
    }

    private Dataset<Row> getTargetDfWithWhereAfterJoin(TargetTable targetTable, Dataset<Row> currentDf) {
        return getTargetDfWithWhere(targetTable, currentDf, WherePlaces.AFTER);
    }

    private Dataset<Row> getTargetDfWithWhere(TargetTable targetTable, Dataset<Row> currentDf, WherePlaces place) {
        List<Column> columnsForWhereBeforeJoin = getColumnsForWhere(targetTable, place);
        Column columnForPreWhere = getColumnForWhere(columnsForWhereBeforeJoin);

        currentDf = currentDf
                .where(
                        columnForPreWhere
                );
        return currentDf;
    }

    private Dataset<Row> getTargetDfWithJoins(TargetTable targetTable, Map<String, Dataset<Row>> sourceDfs, Dataset<Row> currentDf) {
        List<Column> columnsForJoin = getColumnsForJoin(targetTable);
        Column columnForJoin = getColumnForJoin(columnsForJoin);

        currentDf = currentDf
                //TODO удалить sourceDfs.get("second_src_schema@src")
                .join(sourceDfs.get("second_src_schema@src"),
                        columnForJoin,
                        "left"
                );
        return currentDf;
    }

    private Dataset<Row> getResultTargetDf(TargetTable targetTable, Dataset<Row> currentDf) {
        List<Column> columnsForSelect = getColumnsForSelect(targetTable);
        currentDf = currentDf
                .select(
                        columnsForSelect.toArray(new Column[0])
                );
        return currentDf;
    }

    private List<Column> getColumnsForWhere(TargetTable targetTable, WherePlaces place) {
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

    private Column getColumnForWhere(List<Column> columnsForWhere) {
        Column resultColumn = lit("1").equalTo("1");

        if(columnsForWhere.isEmpty()){
            return resultColumn;
        }
        //TODO Реализовать поддержку or
        for(Column column : columnsForWhere) {
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

    private Dataset<Row> getMainSourceDf(TargetTable targetTable, Map<String, Dataset<Row>> sourceDfs) {
        Dataset<Row> sourceDf = sourceDfs.get(sourceDfs.keySet().iterator().next());

        if (sourceDfs.keySet().size() > 1) {
            Join[] joins = targetTable.getClass().getAnnotation(Joins.class).joins();
            sourceDf = sourceDfs.get(getTableAliasName(joins[0].mainTable()));
        }
        return sourceDf;
    }
}
