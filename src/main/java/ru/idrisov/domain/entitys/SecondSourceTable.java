package ru.idrisov.domain.entitys;

import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.EntitySpark;
import ru.idrisov.domain.annotations.PartitionField;
import ru.idrisov.domain.enums.TableSchema;

@Component
@EntitySpark(tableSchema = TableSchema.CUSTOM_FIN_FSO_TMD_STG, tableName = "src_two", filling = false)
public class SecondSourceTable implements TableSpark{
    @PartitionField
    String src_second_field;

    String src_second_field_two;
}
