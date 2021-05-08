package ru.idrisov.datamart;

import org.apache.spark.sql.Column;
import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.SourceTableField;
import ru.idrisov.domain.annotations.WhereCondition;
import ru.idrisov.domain.enums.ColumnValue;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;
import static ru.idrisov.utils.TableUtils.getColumnName;

@Component
public class ColumnWithExpressionCreator {

    public Column getColumnWithExpression(SourceTableField sourceTableInfo, WhereCondition whereCondition) {
        if (rightValueIsEmpty(whereCondition)) {
            return getColumnWithExpressionWithoutValue(sourceTableInfo, whereCondition);
        }
        return getColumnWithExpressionValue(sourceTableInfo, whereCondition);

    }

    private boolean rightValueIsEmpty(WhereCondition whereCondition) {
        return whereCondition.stringRightValue().equals("") && whereCondition.columnRightValue().equals(ColumnValue.none);
    }

    public Column getColumnWithExpressionWithoutValue(SourceTableField sourceTableInfo, WhereCondition whereCondition) {
        switch (whereCondition.type()) {
            case IS_NULL:
                return col(getColumnName(sourceTableInfo)).isNull();
        }
        throw new RuntimeException("Данный тип условия не потдерживается");
    }

    public Column getColumnWithExpressionValue(SourceTableField sourceTableInfo, WhereCondition whereCondition) {
        Object rightValue = getRightValueWithCheckValueType(whereCondition);

        switch (whereCondition.type()) {
            case EQUAL_TO:
                return col(getColumnName(sourceTableInfo)).equalTo(rightValue);
            case LEQ:
                return col(getColumnName(sourceTableInfo)).leq(rightValue);
            case LT:
                return col(getColumnName(sourceTableInfo)).lt(rightValue);
            case GEQ:
                return col(getColumnName(sourceTableInfo)).geq(rightValue);
            case GT:
                return col(getColumnName(sourceTableInfo)).gt(rightValue);
        }
        throw new RuntimeException("Данный тип условия не потдерживается");
    }

    private Object getRightValueWithCheckValueType(WhereCondition whereCondition) {
        Object rightValue = whereCondition.stringRightValue();

        if (!whereCondition.columnRightValue().equals(ColumnValue.none)) {
            rightValue = getColumnForColumnValue(whereCondition);
        }
        return rightValue;
    }

    public Column getColumnForColumnValue(WhereCondition whereCondition) {
        switch (whereCondition.columnRightValue()) {
            case current_timestamp:
                return current_timestamp();
        }
        throw new RuntimeException("Данный метод создания колоннки не потдерживается");
    }
}
