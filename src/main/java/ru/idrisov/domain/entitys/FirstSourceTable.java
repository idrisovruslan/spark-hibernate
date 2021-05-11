package ru.idrisov.domain.entitys;

import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.EntitySpark;
import ru.idrisov.domain.annotations.PartitionField;
import ru.idrisov.domain.enums.TableSchema;

import java.sql.Timestamp;

@Component
@EntitySpark(tableSchema = TableSchema.CUSTOM_FIN_FSO_TMD_STG, tableName = "src_one", filling = false)
public class FirstSourceTable implements TableSpark{
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
