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
import ru.idrisov.domain.entitys.TargetTable;

import static ru.idrisov.utils.TestUtils.*;

@SpringBootTest(classes = SparkTestConfig.class)
@Slf4j
class NewProcessorTest {
    @Autowired
    NewProcessor newProcessor;
    @Autowired
    FirstSourceTable firstSourceTable;
    @Autowired
    TargetTable targetTable;
    @Autowired
    SparkSession sparkSession;

    @Test
    void sparkExample() {
        recreateAllSchemas(sparkSession, firstSourceTable, targetTable);

        Dataset<Row> sourceDf = createRandomSingleRowDf(sparkSession, firstSourceTable);
        createTable(sourceDf, firstSourceTable);

        newProcessor.process();
    }
}