package ru.idrisov.domain.entitys;

import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.EntitySpark;
import ru.idrisov.domain.annotations.PartitionField;

@Component
@EntitySpark(tableSchema = "second_src_schema", tableName = "src", filling = false)
public class SecondSourceTable implements TableSpark{
    @PartitionField
    String src_second_field;

    String src_second_field_two;
}
