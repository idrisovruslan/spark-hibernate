package ru.idrisov.universal_loader;

import org.apache.spark.sql.Column;
import org.springframework.stereotype.Component;
import ru.idrisov.universal_loader.annotations.Aggregate;
import ru.idrisov.universal_loader.annotations.SourceTableField;

import static org.apache.spark.sql.functions.*;
import static ru.idrisov.utils.TableUtils.getColumnName;

@Component
public class ColumnWithAggFunctionCreator {
    public Column getColumnWithAggFunction(Aggregate aggregateInfo, SourceTableField sourceTableInfo) {
        String columnName = getColumnName(sourceTableInfo);
        switch (aggregateInfo.function()) {
            case MAX:
                return max(col(columnName)).as(columnName);
            case MIN:
                return min(col(columnName)).as(columnName);
        }
        throw new RuntimeException("Данный тип условия не потдерживается");
    }
}
