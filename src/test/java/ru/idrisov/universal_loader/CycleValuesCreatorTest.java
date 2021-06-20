package ru.idrisov.universal_loader;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.idrisov.SparkTestConfig;
import ru.idrisov.domain.entitys.dmds.HubAcctTable;
import ru.idrisov.domain.entitys.dmds.HubSyntAcctTable;

import java.util.List;

import static org.apache.spark.sql.functions.lit;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static ru.idrisov.universal_loader.utils.TestUtils.*;

@SpringBootTest(classes = SparkTestConfig.class)
@Slf4j
class CycleValuesCreatorTest {

    @Autowired
    CycleValuesCreator cycleValuesCreator;
    @Autowired
    CycleDfCreator cycleDfCreator;
    @Autowired
    SparkSession sparkSession;
    @Autowired
    HubSyntAcctTable hubSyntAcctTable;
    @Autowired
    HubAcctTable hubAcctTable;

    @Test
    void getCycleValuesList() {

        recreateAllSchemas(sparkSession, hubSyntAcctTable);

        Dataset<Row> sourceDf1 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL1"))
                .withColumn("num", lit("040"));
        Dataset<Row> sourceDf2 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL2"))
                .withColumn("num", lit("04093"));
        Dataset<Row> sourceDf = sourceDf1.union(sourceDf2);
        createTable(sourceDf, hubSyntAcctTable);

        List<CycleValue> list = cycleValuesCreator.getCycleValuesList(hubAcctTable.getClass());
        assertEquals(1, list.size());
        assertEquals("MAIN", list.get(0).getMainCycleName());
        assertEquals("040", list.get(0).getMainCycleValue());
        CycleValue nested = list.get(0).getFirstNotProcessedNestedCycleValue();
        assertEquals("NESTED", nested.getMainCycleName());
        assertEquals("04093", nested.getMainCycleValue());
    }

}