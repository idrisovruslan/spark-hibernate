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
import ru.idrisov.domain.entitys.tests.FirstTargetTable;
import ru.idrisov.domain.entitys.tests.SecondSourceTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.idrisov.universal_loader.utils.TableUtils.readTable;
import static ru.idrisov.universal_loader.utils.TestUtils.*;

@SpringBootTest(classes = SparkTestConfig.class)
@Slf4j
class FirstNewProcessorTest {
    @Autowired
    FirstNewProcessor firstNewProcessor;
    @Autowired
    FirstSourceTable firstSourceTable;
    @Autowired
    SecondSourceTable secondSourceTable;
    @Autowired
    FirstTargetTable firstTargetTable;
    @Autowired
    SparkSession sparkSession;

    @Test
    void defaultTest() {
        recreateAllSchemas(sparkSession, firstSourceTable, secondSourceTable, firstTargetTable);

        Dataset<Row> sourceDf1 = createRandomSingleRowDf(sparkSession, firstSourceTable)
                .withColumn("src_accnt_lvl_1_code", lit("0409301000"))
                .withColumn("src_accnt_sk", lit("0409301000"))
                .withColumn("src_create_date", current_timestamp());
        Dataset<Row> sourceDf2 = createRandomSingleRowDf(sparkSession, firstSourceTable)
                .withColumn("src_accnt_lvl_1_code", lit("0409301000"))
                .withColumn("src_accnt_sk", lit("1111111000"))
                .withColumn("src_create_date", current_timestamp());
        Dataset<Row> sourceDf3 = createRandomSingleRowDf(sparkSession, firstSourceTable)
                .withColumn("src_accnt_lvl_1_code", lit("2222222000"))
                .withColumn("src_create_date", current_timestamp());
        Dataset<Row> sourceDf = sourceDf1.union(sourceDf2).union(sourceDf3);
        createTable(sourceDf, firstSourceTable);

        Dataset<Row> source2Df = createRandomSingleRowDf(sparkSession, secondSourceTable)
                .withColumn("src_second_field", lit("0409301000"));
        createTable(source2Df, secondSourceTable);


        firstNewProcessor.process();


        Dataset<Row> sourceDf1After = readTable(sparkSession, firstSourceTable);
        sourceDf1After.show();
        assertEquals(3, sourceDf1After.count());
        Dataset<Row> sourceDf2After = readTable(sparkSession, secondSourceTable);
        sourceDf2After.show();
        assertEquals(1, sourceDf2After.count());
        Dataset<Row> targetDf = readTable(sparkSession, firstTargetTable);
        targetDf.show();
        assertEquals(2, targetDf.count());
    }

    @Test
    void defaultTest2() {
        Map<Integer, List<String>> map = new HashMap<>();
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");
        map.put(1, list);
        map.get(1).add("4");
        map.get(1).add("5");
        map.get(1).add("6");

        for (String a : map.get(1)) {
            System.out.println(a);
        }
    }
}