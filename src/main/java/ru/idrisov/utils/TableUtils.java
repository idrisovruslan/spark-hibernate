package ru.idrisov.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.idrisov.annotations.EntitySpark;
import ru.idrisov.domain.entitys.TableSpark;

public class TableUtils {
    private static final String DEFAULT_FORMAT = "hive";
    private static final String SEPARATOR = ".";
    private static final String ALIAS_SEPARATOR = "_";

    public static Dataset<Row> readTable(SparkSession sparkSession, TableSpark tableSpark) {
        return readTable(sparkSession, getTableFullName(tableSpark));
    }

    private static Dataset<Row> readTable(SparkSession sparkSession, String tableFullName) {
        return sparkSession.read()
                .format(DEFAULT_FORMAT)
                .table(tableFullName);
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
}
