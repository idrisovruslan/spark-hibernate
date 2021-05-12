 package ru.idrisov.domain.entitys.tmd_stg;

 import lombok.AccessLevel;
 import lombok.experimental.FieldDefaults;
 import org.springframework.stereotype.Component;
 import ru.idrisov.domain.entitys.tmd_stg.default_log_tables.DefaultLogDataTable;
 import ru.idrisov.universal_loader.annotations.EntitySpark;

@Component
@FieldDefaults(level= AccessLevel.PUBLIC)
@EntitySpark(tableSchema = "custom_fin_fso_tmd_stg", tableName = "log_load_dmds_acct_data", filling = true)
public class LogLoadDmdsAcctDataTable extends DefaultLogDataTable {
}