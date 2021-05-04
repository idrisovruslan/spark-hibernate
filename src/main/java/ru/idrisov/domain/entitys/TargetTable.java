package ru.idrisov.domain.entitys;

import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.*;
import ru.idrisov.domain.enums.ConditionType;
import ru.idrisov.domain.enums.JoinTypes;
import ru.idrisov.domain.enums.WherePlace;

import java.sql.Timestamp;

@Component
@EntitySpark(tableSchema = "target_schema", tableName = "target", filling = true)
@Joins(joins = {
        @Join(joinType = JoinTypes.LEFT,
                mainTable = FirstSourceTable.class,
                joinedTable = SecondSourceTable.class,
                joinCondition = {@JoinCondition(mainTableField = "src_accnt_sk", joinedTableField = "src_second_field")}
                )
})
public class TargetTable implements TableSpark{

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_accnt_sk")
    String accnt_sk;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_accnt_status_sk")
    String accnt_status_sk;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_begin_dt")
    Timestamp begin_dt;

    @PartitionField
    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_accnt_lvl_1_code",
            conditions = {@WhereCondition(type = ConditionType.EQUAL_TO, value = "0409301", place = WherePlace.AFTER_JOIN)})
    String accnt_lvl_1_code;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_user_name")
    String user_name;

//    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_create_date",
//            conditions = {@Condition(type = "equalTo", value = "current_timestamp()", valueType = "Column", place = WhereTypes.BEFORE_JOIN)})
//    Timestamp create_date;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_ctl_loading")
    Long ctl_loading;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_ctl_validfrom")
    Timestamp ctl_validfrom;

    @SourceTableField(sourceTable = SecondSourceTable.class, sourceFieldName = "src_second_field")
    String trg_from_second_table_field;
}
