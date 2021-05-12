package ru.idrisov.universal_loader.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import ru.idrisov.universal_loader.annotations.EntitySpark;
import ru.idrisov.universal_loader.annotations.PartitionField;
import ru.idrisov.universal_loader.annotations.SourceTableField;
import ru.idrisov.universal_loader.entitys.TableSpark;

import java.lang.reflect.Field;
import java.util.Arrays;

public class TableUtils {
    private static final String DEFAULT_FORMAT = "hive";
    private static final String SEPARATOR = ".";
    private static final String ALIAS_SEPARATOR = "@";

    public static Dataset<Row> readTable(SparkSession sparkSession, TableSpark tableSpark) {
        return readTable(sparkSession, getTableFullName(tableSpark));
    }

    private static Dataset<Row> readTable(SparkSession sparkSession, String tableFullName) {
        return sparkSession.read()
                .format(DEFAULT_FORMAT)
                .table(tableFullName);
    }

    public static void saveAsTable(Dataset<Row> df, TableSpark tableSpark) {
        String tableFullName = getTableFullName(tableSpark);

        String[] partitionColumnNames = Arrays.stream(tableSpark.getClass(). getFields())
                .filter(declaredField -> {
                    PartitionField partitionFieldAnnotation = declaredField.getAnnotation(PartitionField.class);
                    return partitionFieldAnnotation != null;
                })
                .map(Field::getName)
                .toArray(String[]::new);
        saveAsTable(df, tableFullName, partitionColumnNames);
    }

    private static void saveAsTable(Dataset<Row> df, String tableFullName, String... partitionFieldNames) {
        df.write().mode(SaveMode.Append)
                .partitionBy(partitionFieldNames)
                .format(DEFAULT_FORMAT)
                .saveAsTable(tableFullName);
    }

    public static String getTableFullName(TableSpark tableSpark) {
        return getTableFullName(tableSpark.getClass());
    }

    public static String getTableFullName(Class<? extends TableSpark> tableSparkClass) {
        return getTableFullName(tableSparkClass, SEPARATOR);
    }

    public static String getTableAliasName(Class<? extends TableSpark> tableSparkClass) {
        return getTableFullName(tableSparkClass, ALIAS_SEPARATOR);
    }

    private static String getTableFullName(Class<? extends TableSpark> tableSparkClass, String separator) {
        EntitySpark annotation = tableSparkClass.getAnnotation(EntitySpark.class);
        return annotation.tableSchema() + separator +  annotation.tableName();
    }

    public static String getColumnName(SourceTableField sourceTableInfo) {
        return getColumnName(sourceTableInfo.sourceTable(), sourceTableInfo.sourceFieldName());
    }

    public static String getColumnName(Class<? extends TableSpark> tableSpark, String fieldName) {
        String sourceTableName = getTableAliasName(tableSpark);
        return sourceTableName + "." + fieldName;
    }
}
