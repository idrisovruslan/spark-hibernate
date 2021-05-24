package ru.idrisov.domain.entitys.dmds;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.domain.enums.TableSchema;
import ru.idrisov.universal_loader.annotations.EntitySpark;
import ru.idrisov.universal_loader.annotations.PartitionField;
import ru.idrisov.universal_loader.entitys.TableSpark;

import java.sql.Timestamp;

@Component
@FieldDefaults(level= AccessLevel.PUBLIC)
@EntitySpark(tableSchema = TableSchema.Schema.CUSTOM_FIN_FSO_DMDS, tableName = "hub_synt_acct", filling = false)
public class HubSyntAcct implements TableSpark {
    String syntacct_sk;
    Timestamp open_dt;
    Timestamp close_dt;
    String syntacct_type;
    String num;
    String sid;
    Boolean dirty_flag;
    String gold_sid;
    String instance_code;
    String syntacct_dp_code;
    String begin_dt;
    @PartitionField
    String num_1_3_cd;
    String user_name;
    Timestamp create_date;
    Long ctl_loading;
    Timestamp ctl_validfrom;
}