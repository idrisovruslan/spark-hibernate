package ru.idrisov.domain.entitys.tmd_stg;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.EntitySpark;
import ru.idrisov.domain.annotations.PartitionField;
import ru.idrisov.domain.entitys.TableSpark;
import ru.idrisov.domain.enums.TableSchema;

import java.sql.Timestamp;

@Component
@FieldDefaults(level= AccessLevel.PUBLIC)
public class DefaultLogDataTable implements TableSpark {

    Timestamp create_date;
    Long  ctl_loading;
    Timestamp  ctl_validfrom;
    
    @PartitionField
    String  ctl_validfrom_date;
    
    Long  ctl_loading_src;
    Timestamp  ctl_validfrom_src;
    String  schema_src;
    String  table_name_src;
    String  schema_trg;
    String  table_name_trg;
    String  partition_name_trg;
    String  portion_name_trg;
    Timestamp  begin_ts;
    String  user_name;
    String  ctl_name;
    String  script_name;
}