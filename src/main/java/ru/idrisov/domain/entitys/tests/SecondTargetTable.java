package ru.idrisov.domain.entitys.tests;

import org.springframework.stereotype.Component;
import ru.idrisov.domain.annotations.*;
import ru.idrisov.domain.entitys.TableSpark;
import ru.idrisov.domain.enums.AggregateFunctions;
import ru.idrisov.domain.enums.JoinTypes;
import ru.idrisov.domain.enums.TableSchema;

@Component
@EntitySpark(tableSchema = TableSchema.CUSTOM_FIN_FSO_TMD_STG, tableName = "target_two", filling = true)
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
    public String accnt_sk;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_ctl_loading")
    @Aggregate(function = AggregateFunctions.MAX)
    public Long ctl_loading;

    @SourceTableField(sourceTable = SecondSourceTable.class, sourceFieldName = "src_second_field_two")
    @GroupBy
    public String trg_from_second_table_field;
}
