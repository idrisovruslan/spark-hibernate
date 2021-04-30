package ru.idrisov.datamart;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import ru.idrisov.domain.annotations.Condition;
import ru.idrisov.domain.annotations.SourceTableField;
import ru.idrisov.domain.entitys.TableSpark;
import ru.idrisov.domain.entitys.TargetTable;

import java.util.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static ru.idrisov.utils.TableUtils.*;

@Service
@AllArgsConstructor
public class NewUniversalProcessor {

    ApplicationContext applicationContext;
    SparkSession sparkSession;

    public void fillTable(TargetTable targetTable) {

        Map<String, Dataset<Row>> sourceDfs = getSourceDfsMap(targetTable);

        List<Column> columnsForWhereBeforeJoin = getColumnsForWhere(targetTable, "Before");
        Column columnForPreWhere = getColumnForWhere(columnsForWhereBeforeJoin);

//        List<Column> columnsForJoin = getColumnsForJoin(targetTable);
//        Column columnForJoin = getColumnForJoin(columnsForJoin);

        List<Column> columnsForWhereAfterJoin = getColumnsForWhere(targetTable, "After");
        Column columnForPostWhere = getColumnForWhere(columnsForWhereAfterJoin);

        List<Column> columnsForSelect = getColumnsForSelect(targetTable);

        //TODO удалить sourceDfs.get("first_src_schema@src")
        Dataset<Row> sourceDf = sourceDfs.get("first_src_schema@src");

        sourceDf.show();

        Dataset<Row> targetDf = sourceDf
                .where(
                        columnForPreWhere
                )
                //TODO удалить sourceDfs.get("first_src_schema@src")
//                .join(sourceDfs.get("first_src_schema@src"),
//                        columnForJoin,
//                        "left"
//                )
                .where(
                        columnForPostWhere
                )
                .select(
                        columnsForSelect.toArray(new Column[0])
                );

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

    private List<Column> getColumnsForWhere(TargetTable targetTable, String place) {
        List<Column> columnsForPreWhere = new ArrayList<>();

        Arrays.stream(targetTable.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .filter(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    return Arrays.stream(sourceTableInfo.conditions())
                            .anyMatch(condition -> condition.place().equals(place));
                })
                .forEach(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    Class<? extends TableSpark> sourceTable = sourceTableInfo.sourceTable();
                    String sourceFieldName = sourceTableInfo.sourceFieldName();
                    Condition[] conditions = sourceTableInfo.conditions();

                    Arrays.stream(conditions).forEach(condition -> {
                        //TODO Добавить проверку типа сравнения (equalTo,isin,isNull,leq,lt,geq,gt)
                        //TODO Добавить возможность сравнения с колонками(проверить поле type)


                        Column col = col(getColumnName(sourceTableInfo)).equalTo(condition.value());
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
}
