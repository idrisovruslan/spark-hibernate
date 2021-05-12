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
import ru.idrisov.domain.entitys.tests.SecondTargetTable;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.idrisov.universal_loader.utils.TableUtils.readTable;
import static ru.idrisov.universal_loader.utils.TestUtils.*;

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

    @Test
    void defaultTest() {
        recreateAllSchemas(sparkSession, firstSourceTable, secondSourceTable, secondTargetTable);

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


        Dataset<Row> res = sourceDf.alias("first_src_schema@src")
                .join(source2Df.alias("second_src_schema@src"),
                        col("first_src_schema@src.src_accnt_sk").equalTo(col("second_src_schema@src.src_second_field")),
                        "left"
                )
                .groupBy(
                        col("first_src_schema@src.src_accnt_sk").as("first_src_schema@src.src_accnt_sk"),
                        col("second_src_schema@src.src_second_field_two").as("second_src_schema@src.src_second_field_two")
                )
                .agg(
                        max(col("first_src_schema@src.src_ctl_loading")).as("first_src_schema@src.src_ctl_loading")
                )
                .select(
                        col("`first_src_schema@src.src_accnt_sk`").as("accnt_sk"),
                        col("`first_src_schema@src.src_ctl_loading`").as("ctl_loading"),
                        col("`second_src_schema@src.src_second_field_two`").as("trg_from_second_table_field")
                );
        res.show();
        System.out.println("Ручной вариант");


        secondNewProcessor.process();


        Dataset<Row> sourceDf1After = readTable(sparkSession, firstSourceTable);
        sourceDf1After.show();
        assertEquals(sourceDf1After.count(), 3);
        Dataset<Row> targetDf = readTable(sparkSession, secondTargetTable);
        targetDf.show();
        System.out.println("Авто вариант");
        assertEquals(targetDf.count(), 2);
    }
}