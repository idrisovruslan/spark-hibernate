package ru.idrisov.domain.entitys.tests;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.universal_loader.annotations.*;
import ru.idrisov.universal_loader.entitys.TableSpark;
import ru.idrisov.universal_loader.enums.AggregateFunctions;
import ru.idrisov.universal_loader.enums.JoinTypes;

@Component
@FieldDefaults(level= AccessLevel.PUBLIC)
@EntitySpark(tableSchema = "custom_fin_fso_tmd_stg", tableName = "target_two", filling = true)
@Joins(joins = {
        @Join(joinType = JoinTypes.LEFT,
                mainTable = FirstSourceTable.class,
                joinedTable = SecondSourceTable.class,
                joinCondition = {@JoinCondition(mainTableField = "src_accnt_sk", joinedTableField = "src_second_field")}
        )
})
public class SecondTargetTable implements TableSpark {

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_accnt_sk")
    @GroupBy
    String accnt_sk;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_ctl_loading")
    @Aggregate(function = AggregateFunctions.MAX)
    Long ctl_loading;

    @SourceTableField(sourceTable = SecondSourceTable.class, sourceFieldName = "src_second_field_two")
    @GroupBy
    String trg_from_second_table_field;
}
