package ru.idrisov.datamart;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import ru.idrisov.domain.annotations.Join;
import ru.idrisov.domain.annotations.Joins;
import ru.idrisov.domain.annotations.SourceTableField;
import ru.idrisov.domain.entitys.TableSpark;
import ru.idrisov.domain.entitys.TargetTable;

import java.util.*;

import static ru.idrisov.utils.TableUtils.*;

@Service
@RequiredArgsConstructor
public class NewUniversalProcessor {

    final ColumnCreator columnCreator;
    final DataFrameBuilder dataFrameBuilder;

    public void fillTable(TargetTable targetTable) {
        Dataset<Row> targetDf = dataFrameBuilder.initBuilder(targetTable)
                .addToDfWhereConditionBeforeJoin()
                .addToDfJoins()
                .addToDfWhereConditionAfterJoin()
                .getResultTargetDf();

        saveAsTable(targetDf, targetTable);
    }
}
