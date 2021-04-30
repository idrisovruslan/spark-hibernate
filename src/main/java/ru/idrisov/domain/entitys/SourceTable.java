package ru.idrisov.domain.entitys;

import org.springframework.stereotype.Component;
import ru.idrisov.annotations.EntitySpark;
import ru.idrisov.annotations.PartitionField;

import java.sql.Timestamp;

@Component
@EntitySpark(tableSchema = "src_schema", tableName = "src", filling = false)
public class SourceTable implements TableSpark{
    String src_accnt_sk;

    String src_accnt_status_sk;

    Timestamp src_begin_dt;

    @PartitionField
    String src_accnt_lvl_1_code;

    String src_user_name;

    Timestamp src_create_date;

    Long src_ctl_loading;

    Timestamp src_ctl_validfrom;
}