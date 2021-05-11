package ru.idrisov.domain.entitys.tests;

import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.EntitySpark;
import ru.idrisov.domain.annotations.PartitionField;
import ru.idrisov.domain.entitys.TableSpark;
import ru.idrisov.domain.enums.TableSchema;

import java.sql.Timestamp;

@Component
@EntitySpark(tableSchema = TableSchema.CUSTOM_FIN_FSO_TMD_STG, tableName = "src_one", filling = false)
public class FirstSourceTable implements TableSpark {
    public String src_accnt_sk;

    public String src_accnt_status_sk;

    public Timestamp src_begin_dt;

    @PartitionField
    public String src_accnt_lvl_1_code;

    public String src_user_name;

    public Timestamp src_create_date;

    public Long src_ctl_loading;

    public Timestamp src_ctl_validfrom;
}
