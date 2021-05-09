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
import ru.idrisov.domain.entitys.SecondTargetTable;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.idrisov.utils.TableUtils.readTable;
import static ru.idrisov.utils.TestUtils.*;

@SpringBootTest(classes = SparkTestConfig.class)
@Slf4j
class SecondNewProcessorTest {
    @Autowired
    SecondNewProcessor secondNewProcessor;
    @Autowired
    FirstSourceTable firstSourceTable;
    @Autowired
    SecondSourceTable secondSourceTable;
    @Autowired
    SecondTargetTable secondTargetTable;
    @Autowired
    SparkSession sparkSession;
    @Autowired
    NewUniversalProcessor newUniversalProcessor;

    @Test
    void defaultTest() {
        recreateAllSchemas(sparkSession, firstSourceTable, secondTargetTable);

        Dataset<Row> sourceDf1 = createRandomSingleRowDf(sparkSession, firstSourceTable)
                .withColumn("src_accnt_sk", lit("aaa"))
                .withColumn("src_ctl_loading", lit("111"));
        Dataset<Row> sourceDf2 = createRandomSingleRowDf(sparkSession, firstSourceTable)
                .withColumn("src_accnt_sk", lit("aaa"))
                .withColumn("src_ctl_loading", lit("222"));
        Dataset<Row> sourceDf3 = createRandomSingleRowDf(sparkSession, firstSourceTable)
                .withColumn("src_accnt_sk", lit("bbb"))
                .withColumn("src_ctl_loading", lit("333"));
        Dataset<Row> sourceDf = sourceDf1.union(sourceDf2).union(sourceDf3);
        createTable(sourceDf, firstSourceTable);


        Dataset<Row> source2Df1 = createRandomSingleRowDf(sparkSession, secondSourceTable)
                .withColumn("src_second_field", lit("aaa"))
                .withColumn("src_second_field_two", lit("123"));
        Dataset<Row> source2Df2 = createRandomSingleRowDf(sparkSession, secondSourceTable)
                .withColumn("src_second_field", lit("bbb"))
                .withColumn("src_second_field_two", lit("567"));
        Dataset<Row> source2Df = source2Df1.union(source2Df2);
        createTable(source2Df, secondSourceTable);


        Dataset<Row> res = sourceDf
                .join(source2Df,
                        sourceDf.col("src_accnt_sk").equalTo(source2Df.col("src_second_field")),
                        "left"
                )
                .groupBy(
                        sourceDf.col("src_accnt_sk"),
                        source2Df.col("src_second_field_two")
                )
                .agg(
                        max(sourceDf.col("ctl_loading")).as("ctl_loading")
                )
                .select(
                        col("src_accnt_sk").as("accnt_sk"),
                        col("src_ctl_loading").as("ctl_loading"),
                        col("src_second_field_two").as("trg_from_second_table_field")
                );


        secondNewProcessor.process();


        Dataset<Row> sourceDf1After = readTable(sparkSession, firstSourceTable);
        sourceDf1After.show();
        assertEquals(sourceDf1After.count(), 3);
        Dataset<Row> targetDf = readTable(sparkSession, secondTargetTable);
        targetDf.show();
        assertEquals(targetDf.count(), 2);
    }
}