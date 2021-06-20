package ru.idrisov.universal_loader;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;
import ru.idrisov.universal_loader.annotations.Cycle;

import java.util.List;
import java.util.Map;

import static ru.idrisov.universal_loader.utils.TableUtils.getTableAliasName;
import static ru.idrisov.universal_loader.utils.TableUtils.readTable;

@Service
@RequiredArgsConstructor
public class CycleDfCreator {

    private final SparkSession sparkSession;
    private final ColumnsCreator columnsCreator;

    public Dataset<Row> getCycleDf(Cycle cycle) {

        Map<Integer, List<Column>> columnsForWhereBeforeJoin = columnsCreator.getColumnsForWhereCondition(cycle);
        Column columnForWhere = columnsCreator.getConditionColumnFromColumnsMap(columnsForWhereBeforeJoin);
        List<Column> columnsForSelect = columnsCreator.getColumnsForSelect(cycle);
        String tableAliasName = getTableAliasName(cycle.sourceTableField().sourceTable());
        Dataset<Row> result = readTable(sparkSession, cycle.sourceTableField().sourceTable()).alias(tableAliasName)
                .where(columnForWhere)
                .select(columnsForSelect.toArray(new Column[0]));

        return result;
    }
}
