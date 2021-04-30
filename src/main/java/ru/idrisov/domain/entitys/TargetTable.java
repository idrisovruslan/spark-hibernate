package ru.idrisov.domain.entitys;

import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.Condition;
import ru.idrisov.domain.annotations.EntitySpark;
import ru.idrisov.domain.annotations.PartitionField;
import ru.idrisov.domain.annotations.SourceTableField;

import java.sql.Timestamp;

@Component
@EntitySpark(tableSchema = "target_schema", tableName = "target", filling = true)
public class TargetTable implements TableSpark{

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_accnt_sk")
    String accnt_sk;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_accnt_status_sk")
    String accnt_status_sk;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_begin_dt")
    Timestamp begin_dt;

    @PartitionField
    @SourceTableField(sourceTable = FirstSourceTable.class,
            sourceFieldName = "src_accnt_lvl_1_code",
            conditions = {@Condition(type = "equalTo", value = "0409301", place = "After")})
    String accnt_lvl_1_code;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_user_name")
    String user_name;

//    @SourceTableField(sourceTable = FirstSourceTable.class,
//            sourceFieldName = "src_create_date",
//            conditions = {@Condition(type = "equalTo", value = "current_timestamp()", valueType = "Column", place = "Before")})
//    Timestamp create_date;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_ctl_loading")
    Long ctl_loading;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_ctl_validfrom")
    Timestamp ctl_validfrom;
//
//    @SourceTableField(sourceTable = SecondSourceTable.class, sourceFieldName = "src_second_field")
//    String trg_from_second_table_field;
}
