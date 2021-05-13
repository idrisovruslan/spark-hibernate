package ru.idrisov.domain.entitys.tmd_stg;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.domain.entitys.tmd_stg.default_log_tables.DefaultLogEventTable;
import ru.idrisov.domain.enums.TableSchema;
import ru.idrisov.universal_loader.annotations.EntitySpark;

@Component
@FieldDefaults(level= AccessLevel.PUBLIC)
@EntitySpark(tableSchema = TableSchema.Schema.CUSTOM_FIN_FSO_TMD_STG, tableName = "log_load_dmds_acct_event", filling = true)
public class LogLoadDmdsAcctEventTable extends DefaultLogEventTable {
}