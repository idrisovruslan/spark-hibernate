package ru.idrisov.universal_loader;

import org.apache.spark.sql.Column;
import org.springframework.stereotype.Component;
import ru.idrisov.universal_loader.annotations.Join;
import ru.idrisov.universal_loader.annotations.JoinCondition;
import ru.idrisov.universal_loader.annotations.SourceTableField;
import ru.idrisov.universal_loader.annotations.WhereCondition;
import ru.idrisov.universal_loader.enums.ColumnValue;

import java.util.Arrays;

import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.expr;
import static ru.idrisov.universal_loader.utils.TableUtils.getColumnName;

@Component
public class ColumnWithExpressionCreator {

    public Column getColumnWithExpressionFromWhereCondition(SourceTableField sourceTableFieldInfo, WhereCondition whereCondition) {
        if (rightValueIsEmpty(whereCondition)) {
            return getColumnWithExpressionWithoutValue(sourceTableFieldInfo, whereCondition);
        }
        return getColumnWithExpressionValue(sourceTableFieldInfo, whereCondition);
    }

    public Column getColumnWithExpressionFromJoinCondition(Join join, JoinCondition joinCondition) {
        String mainColumnName = getColumnName(join.mainTable(), joinCondition.mainTableField());
        String joinedColumnName = getColumnName(join.joinedTable(), joinCondition.joinedTableField());
        return expr(String.format(joinCondition.mainTableFunction(), mainColumnName))
                .equalTo(expr(String.format(joinCondition.joinedTableFunction(), joinedColumnName)));
    }

    private boolean rightValueIsEmpty(WhereCondition whereCondition) {
        return whereCondition.stringRightValue().equals("")
                && whereCondition.columnRightValue().equals(ColumnValue.none)
                && whereCondition.arrayStringRightValue().length == 0;
    }

    private Column getColumnWithExpressionWithoutValue(SourceTableField sourceTableInfo, WhereCondition whereCondition) {
        String columnName = getColumnName(sourceTableInfo);
        return expr(String.format(whereCondition.type().getConditionFunction(), columnName));
    }

    private Column getColumnWithExpressionValue(SourceTableField sourceTableInfo, WhereCondition whereCondition) {
        String rightValue = getRightValueWithCheckValueType(whereCondition);
        String leftValueWithFunction = String.format(whereCondition.leftValueFunction(), getColumnName(sourceTableInfo));
        return expr(String.format(whereCondition.type().getConditionFunction(), leftValueWithFunction, rightValue));
    }

    //TODO переписать чтоб было збс
    private String getRightValueWithCheckValueType(WhereCondition whereCondition) {

        if (!whereCondition.stringRightValue().equals("")) {
            if (whereCondition.stringRightValue().charAt(0) == '\'') {
                return whereCondition.stringRightValue();
            }
            return "'" + whereCondition.stringRightValue() + "'";
        }

//TODO добавить поддержку сравнения с функциями колонок

//        if (!whereCondition.columnRightValue().equals(ColumnValue.none)) {
//            rightValue = getColumnForColumnValue(whereCondition);
//        }

        if (whereCondition.arrayStringRightValue().length >= 1) {
            return String.join(",", Arrays.stream(whereCondition.arrayStringRightValue()).map(x -> "'" + x + "'").toArray(String[]::new));
        }
        throw new RuntimeException("Правое значение не обработано");
    }

    private Column getColumnForColumnValue(WhereCondition whereCondition) {
        switch (whereCondition.columnRightValue()) {
            case current_timestamp:
                return current_timestamp();
        }
        throw new RuntimeException("Данный метод создания колоннки не потдерживается");
    }
}
