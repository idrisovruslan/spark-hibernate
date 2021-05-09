package ru.idrisov.datamart;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;
import ru.idrisov.domain.entitys.TableSpark;

import static ru.idrisov.utils.TableUtils.saveAsTable;

@Service
@RequiredArgsConstructor
public class NewUniversalProcessor {

    final DataFrameBuilder dataFrameBuilder;

    public void fillTable(TableSpark targetTable) {
        Dataset<Row> targetDf = dataFrameBuilder.initBuilder(targetTable)
                .addToDfWhereConditionBeforeJoin()
                .addToDfJoins()
                .addToDfWhereConditionAfterJoin()
                .getResultTargetDf();

        saveAsTable(targetDf, targetTable);
    }
}
