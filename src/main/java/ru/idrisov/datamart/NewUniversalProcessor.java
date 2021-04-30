package ru.idrisov.datamart;

import lombok.AllArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import ru.idrisov.domain.annotations.SourceTableField;
import ru.idrisov.domain.entitys.TableSpark;
import ru.idrisov.domain.entitys.TargetTable;

import java.lang.reflect.Field;
import java.util.*;

import static org.apache.spark.sql.functions.col;
import static ru.idrisov.utils.TableUtils.*;

@Service
@AllArgsConstructor
//TODO удалить Dataset<Row> sourceDf = sourcesDFs.get("first_src_schema@src");
public class NewUniversalProcessor {

    ApplicationContext applicationContext;
    SparkSession sparkSession;

    public void fillTable(TargetTable targetTable) {

        Map<String, Dataset<Row>> sourceDfs = getSourceDfsMap(targetTable);

        List<Column> columnsForSelect = getColumnsForSelect(targetTable);

        Dataset<Row> sourceDf = sourceDfs.get("first_src_schema@src");

        sourceDf.show();

        Dataset<Row> targetDf = sourceDf
                .select(
                        columnsForSelect.toArray(new Column[0])
                );

        targetDf.show();

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

    private List<Column> getColumnsForSelect(TargetTable targetTable) {
        List<Column> listForSelect = new ArrayList<>();

        Arrays.stream(targetTable.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(SourceTableField.class))
                .forEach(field -> {
                    SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
                    String sourceTableName = getTableAliasName(sourceTableInfo.sourceTable());
                    String sourceFieldName = sourceTableInfo.sourceFieldName();
                    String targetFieldName = field.getName();

                    Column col = col(sourceTableName + "." + sourceFieldName).as(targetFieldName);
                    listForSelect.add(col);
                });
        return listForSelect;
    }
}
