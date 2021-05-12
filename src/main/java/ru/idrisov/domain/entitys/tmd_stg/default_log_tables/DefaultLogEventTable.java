package ru.idrisov.domain.entitys.tmd_stg.default_log_tables;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.universal_loader.annotations.PartitionField;
import ru.idrisov.universal_loader.entitys.TableSpark;

import java.sql.Timestamp;

@Component
@FieldDefaults(level= AccessLevel.PUBLIC)
public class DefaultLogEventTable implements TableSpark {

    Long event_code;
    String event_desc;
    Timestamp create_date;
    Long ctl_loading;
    Timestamp ctl_validfrom;

    @PartitionField
    String ctl_validfrom_date;

    Long ctl_loading_src;
    Timestamp ctl_validfrom_src;
    String schema_src;
    String table_name_src;
    String schema_trg;
    String table_name_trg;
    String partition_name_trg;
    String portion_name_trg;
    Timestamp begin_ts;
    String user_name;
    String ctl_name;
    String script_name;
}