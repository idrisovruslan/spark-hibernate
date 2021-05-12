package ru.idrisov.domain.entitys.tmd_stg;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.universal_loader.annotations.EntitySpark;
import ru.idrisov.universal_loader.annotations.PartitionField;
import ru.idrisov.universal_loader.entitys.TableSpark;

import java.sql.Timestamp;

@Component
@FieldDefaults(level= AccessLevel.PUBLIC)
@EntitySpark(tableSchema = "custom_fin_fso_tmd_stg", tableName = "log_load_stg_acct_status", filling = false)
public class LogLoadStgAcctStatusTable implements TableSpark {

     Timestamp create_date;
     Long ctl_loading;
     Timestamp ctl_validfrom;

     @PartitionField
     String ctl_validfrom_date;

     Long ctl_loading_src;
     Timestamp ctl_validfrom_src;
     String load_status;
     String schema_src;
     String table_name_src;
     String schema_trg;
     String table_name_trg;
     String partition_name_trg;
     String portion_name_trg;
     Timestamp begin_ts;
     Timestamp end_ts;
     String user_name;
     String ctl_name;
     String script_name;
}