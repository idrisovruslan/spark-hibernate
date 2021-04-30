package ru.idrisov.domain.entitys;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.idrisov.annotations.EntitySpark;
import ru.idrisov.annotations.PartitionField;
import ru.idrisov.annotations.SourceTableField;

import java.sql.Timestamp;

@Component
@EntitySpark(tableSchema = "target_schema", tableName = "target", filling = true)
@RequiredArgsConstructor
public class TargetTable implements TableSpark{

    @SourceTableField(sourceTable = SourceTable.class, sourceFieldName = "src_accnt_sk")
    String accnt_sk;

    @SourceTableField(sourceTable = SourceTable.class, sourceFieldName = "src_accnt_status_sk")
    String accnt_status_sk;

    @SourceTableField(sourceTable = SourceTable.class, sourceFieldName = "src_begin_dt")
    Timestamp begin_dt;

    @PartitionField
    @SourceTableField(sourceTable = SourceTable.class, sourceFieldName = "src_accnt_lvl_1_code")
    String accnt_lvl_1_code;

    @SourceTableField(sourceTable = SourceTable.class, sourceFieldName = "src_user_name")
    String user_name;

    @SourceTableField(sourceTable = SourceTable.class, sourceFieldName = "src_create_date")
    Timestamp create_date;

    @SourceTableField(sourceTable = SourceTable.class, sourceFieldName = "src_ctl_loading")
    Long ctl_loading;

    @SourceTableField(sourceTable = SourceTable.class, sourceFieldName = "src_ctl_validfrom")
    Timestamp ctl_validfrom;
}
