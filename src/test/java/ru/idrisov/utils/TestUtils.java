package ru.idrisov.utils;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import ru.idrisov.universal_loader.annotations.EntitySpark;
import ru.idrisov.universal_loader.annotations.PartitionField;
import ru.idrisov.universal_loader.entitys.TableSpark;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.idrisov.utils.TableUtils.getTableFullName;

public class TestUtils {
    private static final String DEFAULT_FORMAT = "hive";
    private static final String DROP_SCHEMA_SQL_PATTERN = "DROP SCHEMA IF EXISTS %s CASCADE";
    private static final String CREATE_SCHEMA_SQL_PATTERN = "CREATE SCHEMA %s";

    public static void createEmptyTable(SparkSession sparkSession, TableSpark tableSpark) {
        Dataset<Row> df = sparkSession.createDataFrame(Collections.emptyList(), createTableStruct(tableSpark)); //StructType
        createTable(df, tableSpark);
    }

    public static void createTable(Dataset<Row> df, TableSpark tableSpark) {
        String tableFullName = getTableFullName(tableSpark);

        String[] partitionColumnNames = Arrays.stream(tableSpark.getClass(). getFields())
                .filter(declaredField -> declaredField.isAnnotationPresent(PartitionField.class))
                .map(Field::getName)
                .toArray(String[]::new);
        createTable(df, tableFullName, partitionColumnNames);
    }

    private static void createTable(Dataset<Row> df, String tableName, String[] partitionColumnNames) {
        df.write().mode(SaveMode.Overwrite)
                .format(DEFAULT_FORMAT)
                .partitionBy(partitionColumnNames)
                .saveAsTable(tableName);
    }

    public static void recreateAllSchemas(SparkSession sparkSession, TableSpark... tablesSpark) {
        List<String> schemaNames = Stream.of(tablesSpark)
                .map(tableSpark -> tableSpark.getClass().getAnnotation(EntitySpark.class).tableSchema())
                .distinct().collect(Collectors.toList());

        schemaNames.forEach(schemaName -> {
            sparkSession.sql(String.format(DROP_SCHEMA_SQL_PATTERN, schemaName));
            sparkSession.sql(String.format(CREATE_SCHEMA_SQL_PATTERN, schemaName));
        });
    }

    private static StructType createTableStruct(TableSpark tableSpark) {
        StructType structType = new StructType();
        for (Field declaredField : tableSpark.getClass(). getFields()) {
            structType = structType.add(declaredField.getName(), getDataTypeFromJavaType(declaredField.getType()));
        }
        return structType;
    }

    private static DataType getDataTypeFromJavaType(Class<?> typeField) {
        if (typeField.equals(String.class)){
            return DataTypes.StringType;
        } else if (typeField.equals(Timestamp.class)) {
            return DataTypes.TimestampType;
        } else if (typeField.equals(Long.class)) {
            return DataTypes.LongType;
        }
        throw new RuntimeException("Нет такого типа");
    }

    public static Dataset<Row> createRandomSingleRowDf(SparkSession sparkSession, TableSpark tableSpark) {
        StructType schema = createTableStruct(tableSpark);
        return sparkSession.createDataFrame(createRowsList(schema, 1, Collections.emptyMap()), schema);
    }

    private static List<Row> createRowsList(StructType schema, int size, Map<String, Object> fieldValues) {
        List<Row> result = new ArrayList<>();
        for (int rowNum = 0; rowNum < size; rowNum++) {
            StructField[] fields = schema.fields();
            Object[] values = new Object[fields.length];

            int i = 0;
            for (StructField field : fields) {
                Object value;
                if (fieldValues.containsKey(field.name())) {
                    value = fieldValues.get(field.name());
                } else {
                    value = createRandomValue(field);
                }
                values[i++] = value;
            }

            Row randomRow = RowFactory.create(values);
            result.add(randomRow);
        }
        return result;
    }

    private static Object createRandomValue(StructField field) {
        Object value;
        if (field.dataType().equals(DataTypes.StringType)) {
            value = RandomStringUtils.randomAlphabetic(5);
        } else if (field.dataType().equals(DataTypes.TimestampType)) {
            long randomEpochDay = LocalDate.of(2000, 1, 1).toEpochDay() + 10000L;
            long randomTime = (long) Math.floor(Math.random() * 1000000000L);
            value = new Timestamp(randomEpochDay + randomTime);
        } else if (field.dataType().equals(DataTypes.StringType)) {
            long randomEpochDay = LocalDate.of(2000, 1, 1).toEpochDay() /*+ (long) Math.floor(Math.random() * 10000L)*/;
            value = java.sql.Date.valueOf(LocalDate.ofEpochDay(randomEpochDay));
        } else if (field.dataType() instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) field.dataType();
            value = BigDecimal.valueOf(Math.floor(Math.random() * decimalType.precision())).setScale(0, RoundingMode.FLOOR);
        } else if (field.dataType().equals(DataTypes.IntegerType)) {
            value = (int) Math.floor(Math.random() * 1000);
        } else if (field.dataType().equals(DataTypes.LongType)) {
            value = (long) Math.floor(Math.random() * 1000);
        } else if (field.dataType().equals(DataTypes.FloatType)) {
            value = (float) Math.random() * 1000;
        } else if (field.dataType().equals(DataTypes.BooleanType)) {
            value = Math.random() >= 0.5;
        } else {
            throw new RuntimeException("Unsupported dataType in row: " + field.dataType().typeName());
        }
        return value;
    }
}
