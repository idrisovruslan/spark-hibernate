package ru.idrisov.datamart;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.junit.jupiter.api.Test;
import ru.idrisov.Starter;
import ru.idrisov.domain.entitys.SourceTable;
import ru.idrisov.domain.entitys.TargetTable;

import static org.apache.spark.sql.functions.lit;
import static ru.idrisov.utils.TestUtils.*;

@SpringBootTest(classes = {Starter.class})
@Slf4j
class NewProcessorTest {
    @Autowired
    NewProcessor newProcessor;
    @Autowired
    SourceTable sourceTable;
    @Autowired
    TargetTable targetTable;
    @Autowired
    SparkSession sparkSession;

    @Test
    void sparkExample() {
        recreateAllSchemas(sparkSession, sourceTable, targetTable);

        Dataset<Row> sourceDf = createRandomSingleRowDf(sparkSession, sourceTable);
        createTable(sourceDf, sourceTable);

        newProcessor.process();
    }
}