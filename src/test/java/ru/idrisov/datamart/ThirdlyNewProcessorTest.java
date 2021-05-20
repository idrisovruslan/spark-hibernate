package ru.idrisov.datamart;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.idrisov.SparkTestConfig;
import ru.idrisov.domain.entitys.tests.FirstSourceTable;
import ru.idrisov.domain.entitys.tests.SecondSourceTable;
import ru.idrisov.domain.entitys.tests.ThirdlyTargetTable;

import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.idrisov.universal_loader.utils.TableUtils.readTable;
import static ru.idrisov.universal_loader.utils.TestUtils.*;

@SpringBootTest(classes = SparkTestConfig.class)
@Slf4j
class ThirdlyNewProcessorTest {
    @Autowired
    ThirdlyNewProcessor thirdlyNewProcessor;
    @Autowired
    FirstSourceTable firstSourceTable;
    @Autowired
    ThirdlyTargetTable thirdlyTargetTable;
    @Autowired
    SparkSession sparkSession;

    @Test
    void defaultTest() {
        recreateAllSchemas(sparkSession, firstSourceTable, thirdlyTargetTable);

        Dataset<Row> sourceDf1 = createRandomSingleRowDf(sparkSession, firstSourceTable)
                .withColumn("src_accnt_lvl_1_code", lit("00000"))
                .withColumn("src_accnt_sk", lit("321"))
                .withColumn("src_user_name", lit("Женя"));
        Dataset<Row> sourceDf2 = createRandomSingleRowDf(sparkSession, firstSourceTable)
                .withColumn("src_accnt_lvl_1_code", lit("0409301000"))
                .withColumn("src_accnt_sk", lit("321"))
                .withColumn("src_user_name", lit("Миша"));
        Dataset<Row> sourceDf3 = createRandomSingleRowDf(sparkSession, firstSourceTable)
                .withColumn("src_accnt_lvl_1_code", lit("0409301000"))
                .withColumn("src_accnt_sk", lit("00000"))
                .withColumn("src_user_name", lit("Женя"));
        Dataset<Row> sourceDf = sourceDf1.union(sourceDf2).union(sourceDf3);
        createTable(sourceDf, firstSourceTable);


        thirdlyNewProcessor.process();


        Dataset<Row> sourceDf1After = readTable(sparkSession, firstSourceTable);
        sourceDf1After.show();
        assertEquals(3, sourceDf1After.count());
        Dataset<Row> targetDf = readTable(sparkSession, thirdlyTargetTable);
        targetDf.show();
        assertEquals(2, targetDf.count());
    }
}