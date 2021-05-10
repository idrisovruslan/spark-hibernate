package ru.idrisov.datamart.universal_loader;

import org.apache.spark.sql.Column;
import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.SourceTableField;

import static org.apache.spark.sql.functions.col;

@Component
public class ColumnWithAggFunctionCreator {

    public Column getColumnWithAggFunction(SourceTableField sourceTableInfo, String columnName) {
        return getColumnWithAggFunction(sourceTableInfo, columnName, columnName);
    }

    public Column getColumnWithAggFunction(SourceTableField sourceTableInfo, String columnName, String newColumnName) {
        return col(columnName).as(newColumnName);
    }
}
