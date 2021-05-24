package ru.idrisov.universal_loader;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Service;
import ru.idrisov.universal_loader.entitys.TableSpark;

import static ru.idrisov.universal_loader.utils.TableUtils.saveAsTable;

@Service
@RequiredArgsConstructor
public class NewUniversalProcessor {

    final DataFrameBuilder dataFrameBuilder;

    public void fillDfAndSaveToTable(TableSpark targetTable) {
        Dataset<Row> targetDf = getFilledDf(targetTable);
        saveAsTable(targetDf, targetTable);
    }

    public Dataset<Row> getFilledDf(TableSpark targetTable) {
        return dataFrameBuilder.initBuilder(targetTable)
                .addToDfWhereConditionOnStart()
                .addToDfWhereConditionBeforeJoin()
                .addToDfJoins()
                .addToDfWhereConditionAfterJoin()
                .addToDfAggregateFunctions()
                .addToDfGroupByWithAggFunctions()

                //TODO проверить работоспособность
                .addToDfWhereConditionAfterGroupBy()

                .addToDfDistinct()
                .createResultTargetDf()
                .getResultTargetDf();
    }
}
