package ru.idrisov.universal_loader;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import ru.idrisov.SparkTestConfig;
import ru.idrisov.domain.entitys.dmds.HubSyntAcctTable;
import ru.idrisov.domain.entitys.tests.CycleTestTable;

import java.util.ArrayList;
import java.util.Arrays;
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
    CycleTestTable cycleTestTable;

    @Test
    void getCycleValuesList() {
        recreateAllSchemas(sparkSession, hubSyntAcctTable);
        createHubSyntAcctTable();

        List<CycleValue> list = cycleValuesCreator.getCycleValuesList(cycleTestTable.getClass());
        CycleValue mainCycle = list.get(0);

        assertEquals(1, list.size());

        List<String> allValuesList = new ArrayList<String>(Arrays.asList("040","04093","04041","04086","0409311","0409322","0409333","0404111","0404122","0404133","0408611","0408622","0408633"));

        assertEquals("MAIN", mainCycle.getMainCycleName());
        allValuesList.remove(mainCycle.getMainCycleValue());

        //Порядок важен
        CycleValue nested1_1 = checkValue(allValuesList, mainCycle, "NESTED", 0);
        CycleValue nested1_1_1 = checkValue(allValuesList, nested1_1, "NESTED_NESTED", 0);
        CycleValue nested1_1_2 = checkValue(allValuesList, nested1_1, "NESTED_NESTED", 1);
        CycleValue nested1_1_3 = checkValue(allValuesList, nested1_1, "NESTED_NESTED", 2);

        CycleValue nested1_2 = checkValue(allValuesList, mainCycle, "NESTED", 1);
        CycleValue nested1_2_1 = checkValue(allValuesList, nested1_2, "NESTED_NESTED", 0);
        CycleValue nested1_2_2 = checkValue(allValuesList, nested1_2, "NESTED_NESTED", 1);
        CycleValue nested1_2_3 = checkValue(allValuesList, nested1_2, "NESTED_NESTED", 2);

        CycleValue nested1_3 = checkValue(allValuesList, mainCycle, "NESTED", 2);
        CycleValue nested1_3_1 = checkValue(allValuesList, nested1_3, "NESTED_NESTED", 0);
        CycleValue nested1_3_2 = checkValue(allValuesList, nested1_3, "NESTED_NESTED", 1);
        CycleValue nested1_3_3 = checkValue(allValuesList, nested1_3, "NESTED_NESTED", 2);
        assertEquals(0, allValuesList.size());
    }

    private CycleValue checkValue(List<String> allValuesList, CycleValue mainCycleValue, String cycleName, int index) {
        CycleValue nestedCycleValue = mainCycleValue.getNestedCycleValueFromIndex(index);

        assertEquals(cycleName, nestedCycleValue.getMainCycleName());
        allValuesList.remove(nestedCycleValue.getMainCycleValue());
        return nestedCycleValue;
    }

    private void createHubSyntAcctTable() {
        Dataset<Row> sourceDf1 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL1"))
                .withColumn("num", lit("040"));
        Dataset<Row> sourceDf1_1 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL2"))
                .withColumn("num", lit("04093"));
        Dataset<Row> sourceDf1_2 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL2"))
                .withColumn("num", lit("04041"));
        Dataset<Row> sourceDf1_3 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL2"))
                .withColumn("num", lit("04086"));
        Dataset<Row> sourceDf1_1_1 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL3"))
                .withColumn("num", lit("0409311"));
        Dataset<Row> sourceDf1_1_2 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL3"))
                .withColumn("num", lit("0409322"));
        Dataset<Row> sourceDf1_1_3 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL3"))
                .withColumn("num", lit("0409333"));
        Dataset<Row> sourceDf1_2_1 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL3"))
                .withColumn("num", lit("0404111"));
        Dataset<Row> sourceDf1_2_2 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL3"))
                .withColumn("num", lit("0404122"));
        Dataset<Row> sourceDf1_2_3 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL3"))
                .withColumn("num", lit("0404133"));
        Dataset<Row> sourceDf1_3_1 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL3"))
                .withColumn("num", lit("0408611"));
        Dataset<Row> sourceDf1_3_2 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL3"))
                .withColumn("num", lit("0408622"));
        Dataset<Row> sourceDf1_3_3 = createRandomSingleRowDf(sparkSession, hubSyntAcctTable)
                .withColumn("syntacct_type", lit("LVL3"))
                .withColumn("num", lit("0408633"));

        Dataset<Row> sourceDf = sourceDf1
                .union(sourceDf1_1)
                .union(sourceDf1_2)
                .union(sourceDf1_3)
                .union(sourceDf1_1_1)
                .union(sourceDf1_1_2)
                .union(sourceDf1_1_3)
                .union(sourceDf1_2_1)
                .union(sourceDf1_2_2)
                .union(sourceDf1_2_3)
                .union(sourceDf1_3_1)
                .union(sourceDf1_3_2)
                .union(sourceDf1_3_3);
        createTable(sourceDf, hubSyntAcctTable);
    }
}