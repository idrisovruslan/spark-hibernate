package ru.idrisov.domain.entitys.tests;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.EntitySpark;
import ru.idrisov.domain.annotations.PartitionField;
import ru.idrisov.domain.entitys.TableSpark;
import ru.idrisov.domain.enums.TableSchema;

@Component
@FieldDefaults(level= AccessLevel.PUBLIC)
@EntitySpark(tableSchema = TableSchema.CUSTOM_FIN_FSO_TMD_STG, tableName = "src_two", filling = false)
public class SecondSourceTable implements TableSpark {
    @PartitionField
    String src_second_field;

    String src_second_field_two;
}
