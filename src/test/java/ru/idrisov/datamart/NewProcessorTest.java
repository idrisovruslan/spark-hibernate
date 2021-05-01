package ru.idrisov.datamart;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.idrisov.SparkTestConfig;
import ru.idrisov.domain.entitys.FirstSourceTable;
import ru.idrisov.domain.entitys.SecondSourceTable;
import ru.idrisov.domain.entitys.TargetTable;

import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.idrisov.utils.TableUtils.readTable;
import static ru.idrisov.utils.TestUtils.*;

@SpringBootTest(classes = SparkTestConfig.class)
@Slf4j
class NewProcessorTest {
    @Autowired
    NewProcessor newProcessor;
    @Autowired
    FirstSourceTable firstSourceTable;
    @Autowired
    SecondSourceTable secondSourceTable;
    @Autowired
    TargetTable targetTable;
    @Autowired
    SparkSession sparkSession;
    @Autowired
    NewUniversalProcessor newUniversalProcessor;

    @Test
    void sparkExample() {
        recreateAllSchemas(sparkSession, firstSourceTable, targetTable);

        Dataset<Row> sourceDf1 = createRandomSingleRowDf(sparkSession, firstSourceTable)
                .withColumn("src_accnt_lvl_1_code", lit("0409301"))
                .withColumn("src_accnt_sk", lit("0409301"))
                .withColumn("src_create_date", current_timestamp());
        Dataset<Row> sourceDf2 = createRandomSingleRowDf(sparkSession, firstSourceTable)
                .withColumn("src_accnt_lvl_1_code", lit("0409301"))
                .withColumn("src_accnt_sk", lit("1111111"))
                .withColumn("src_create_date", current_timestamp());
        Dataset<Row> sourceDf3 = createRandomSingleRowDf(sparkSession, firstSourceTable)
                .withColumn("src_accnt_lvl_1_code", lit("2222222"))
                .withColumn("src_create_date", current_timestamp());
        Dataset<Row> sourceDf = sourceDf1.union(sourceDf2).union(sourceDf3);
        createTable(sourceDf, firstSourceTable);

        Dataset<Row> source2Df = createRandomSingleRowDf(sparkSession, secondSourceTable)
                .withColumn("src_second_field", lit("0409301"));
        createTable(source2Df, secondSourceTable);


        newProcessor.process();


        Dataset<Row> sourceDfAfter = readTable(sparkSession, firstSourceTable);
        assertEquals(sourceDfAfter.count(), 3);
        Dataset<Row> targetDf = readTable(sparkSession, targetTable);
        assertEquals(targetDf.count(), 2);
    }
}