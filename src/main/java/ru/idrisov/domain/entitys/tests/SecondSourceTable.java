package ru.idrisov.domain.entitys.tests;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.universal_loader.annotations.EntitySpark;
import ru.idrisov.universal_loader.annotations.PartitionField;
import ru.idrisov.universal_loader.entitys.TableSpark;

@Component
@FieldDefaults(level= AccessLevel.PUBLIC)
@EntitySpark(tableSchema = "custom_fin_fso_tmd_stg", tableName = "src_two", filling = false)
public class SecondSourceTable implements TableSpark {
    @PartitionField
    String src_second_field;

    String src_second_field_two;
}
