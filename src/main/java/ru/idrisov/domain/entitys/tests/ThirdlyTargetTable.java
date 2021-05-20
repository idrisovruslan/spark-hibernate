package ru.idrisov.domain.entitys.tests;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.domain.enums.TableSchema;
import ru.idrisov.universal_loader.annotations.*;
import ru.idrisov.universal_loader.entitys.TableSpark;
import ru.idrisov.universal_loader.enums.ConditionType;
import ru.idrisov.universal_loader.enums.WherePlace;

import java.sql.Timestamp;

@Component
@FieldDefaults(level= AccessLevel.PUBLIC)
@EntitySpark(tableSchema = TableSchema.Schema.CUSTOM_FIN_FSO_STG, tableName = "target_three", filling = true)
@Distinct
public class ThirdlyTargetTable implements TableSpark {

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_accnt_sk", function = "substring(%s, 0, 3)",
            conditions = {@WhereCondition(type = ConditionType.EQUAL_TO, stringRightValue = "321", place = WherePlace.AFTER_JOIN, orGroup = 2)})
    String accnt_sk;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_accnt_status_sk")
    String accnt_status_sk;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_begin_dt")
    Timestamp begin_dt;

    @PartitionField
    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_accnt_lvl_1_code",
            conditions = {@WhereCondition(type = ConditionType.IS_IN, leftValueFunction = "substring(%s, 0, 7)", arrayStringRightValue = {"0409301", "0409302"}, place = WherePlace.AFTER_JOIN, orGroup = 1)})
    String accnt_lvl_1_code;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_user_name", function = "substring(%s, 0, 5)",
            conditions = {@WhereCondition(type = ConditionType.EQUAL_TO, leftValueFunction = "substring(%s, 0, 4)", stringRightValue = "Женя", place = WherePlace.AFTER_JOIN, orGroup = 1)})
    String user_name;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_ctl_loading")
    Long ctl_loading;

    @SourceTableField(sourceTable = FirstSourceTable.class, sourceFieldName = "src_ctl_validfrom")
    Timestamp ctl_validfrom;
}
