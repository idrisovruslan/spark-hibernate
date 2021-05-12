package ru.idrisov.domain.entitys.tests;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.universal_loader.annotations.EntitySpark;
import ru.idrisov.universal_loader.annotations.PartitionField;
import ru.idrisov.universal_loader.entitys.TableSpark;

import java.sql.Timestamp;

@Component
@FieldDefaults(level= AccessLevel.PUBLIC)
@EntitySpark(tableSchema = "custom_fin_fso_tmd_stg", tableName = "src_one", filling = false)
public class FirstSourceTable implements TableSpark {
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
