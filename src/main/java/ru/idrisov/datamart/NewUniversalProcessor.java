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

        Map<String, Dataset<Row>> sourcesDFs = new HashMap<>();
        for (Class<? extends TableSpark> clazz: getSourceTables(targetTable)) {
            String tableAliasName = getTableAliasName(clazz);

            Dataset<Row> sourceDf = readTable(sparkSession, applicationContext.getBean(clazz)).alias(tableAliasName);
            sourcesDFs.put(tableAliasName, sourceDf);
        }

        List<Column> listForSelect = new ArrayList<>();

        for (Field field : targetTable.getClass().getDeclaredFields()) {
            SourceTableField sourceTableInfo = field.getAnnotation(SourceTableField.class);
            String sourceTableName = getTableAliasName(sourceTableInfo.sourceTable());
            String sourceFieldName = sourceTableInfo.sourceFieldName();
            String targetFieldName = field.getName();

            Column col = col(sourceTableName + "." + sourceFieldName).as(targetFieldName);
            listForSelect.add(col);
        }

        Dataset<Row> sourceDf = sourcesDFs.get("first_src_schema@src");

        sourceDf.show();

        Dataset<Row> targetDf = sourceDf
                .select(
                        listForSelect.toArray(new Column[0])
                );

        targetDf.show();

    }
    private Set<Class<? extends TableSpark>> getSourceTables(TargetTable targetTable) {
        Set<Class<? extends TableSpark>> set = new HashSet<>();
        Arrays.stream(targetTable.getClass().getDeclaredFields())
                .forEach(field -> set.add(field.getAnnotation(SourceTableField.class).sourceTable()));
        return set;
    }
}
