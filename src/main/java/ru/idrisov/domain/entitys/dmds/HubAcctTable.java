package ru.idrisov.domain.entitys.dmds;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.domain.entitys.stg.ReplEssAcctTable;
import ru.idrisov.domain.enums.TableSchema;
import ru.idrisov.universal_loader.annotations.*;
import ru.idrisov.universal_loader.entitys.TableSpark;
import ru.idrisov.universal_loader.enums.ConditionType;
import ru.idrisov.universal_loader.enums.JoinTypes;
import ru.idrisov.universal_loader.enums.WherePlace;

import java.sql.Timestamp;

@Getter
@Component
@FieldDefaults(level = AccessLevel.PUBLIC)
@EntitySpark(tableSchema = TableSchema.Schema.CUSTOM_FIN_FSO_DMDS, tableName = "hub_acct", filling = false)
@Joins(joins = {
        @Join(
                joinType = JoinTypes.LEFT,
                mainTable = ReplEssAcctTable.class,
                joinedTable = HubAcctTable.class,
                joinCondition = {@JoinCondition(mainTableField = "acct_sk", joinedTableField = "acct_sk")}
        )
})
@WhereConditions(conditionsFields = {
        @SourceTableField(sourceTable = HubAcctTable.class, sourceFieldName = "acct_sk",
                conditions = {@WhereCondition(type = ConditionType.IS_NULL, place = WherePlace.AFTER_JOIN)}),
        @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "ctl_loading",
                //TODO Добавить поддержку подстановки своих значений
                conditions = {@WhereCondition(type = ConditionType.GT, stringRightValue = "0409302", place = WherePlace.AFTER_JOIN)}),
        @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "ctl_loading_date",
                //TODO Добавить поддержку подстановки своих значений
                conditions = {@WhereCondition(type = ConditionType.IS_IN, arrayStringRightValue = "0409302", place = WherePlace.AFTER_JOIN)})
})
@Cycle(table = HubSyntAcct.class, column = "num", cycleName = "MAIN",
        conditions = {@WhereCondition(stringRightValue = "LVL1")})
@Cycle(table = HubSyntAcct.class, column = "num", cycleName = "NOT_MAIN :)",
        conditions = {@WhereCondition(stringRightValue = "LVL2"),
                @WhereCondition(leftValueFunction = "substring(%s, 1, 3)", cycleRightValue = "MAIN")})
@Distinct
public class HubAcctTable implements TableSpark {

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "acct_sk")
    String acct_sk;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "cnsld_flag")
    Boolean cnsld_flag;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "crncy_sk")
    String crncy_sk;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "crncy_dp_code")
    String crncy_dp_code;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "depo_subj_sk")
    String depo_subj_sk;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "depo_subj_dp_code")
    String depo_subj_dp_code;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "dirty_flag")
    Boolean dirty_flag;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "escrow_flag")
    Boolean escrow_flag;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "frsymb_sk")
    String frsymb_sk;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "frsymb_dp_code")
    String frsymb_dp_code;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "gold_sid")
    String gold_sid;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "instance_code")
    String instance_code;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "name")
    String name;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "ntf_tax_open_dt")
    Timestamp ntf_tax_open_dt;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "num",
            conditions = {
            //TODO Добавить поддержку подстановки своих значений
            @WhereCondition(leftValueFunction = "substring(%s, 1, 3)", cycleRightValue = "MAIN", place = WherePlace.AFTER_JOIN),
            @WhereCondition(leftValueFunction = "substring(%s, 1, 5)", cycleRightValue = "NOT_MAIN :)", place = WherePlace.AFTER_JOIN)
    })
    String num;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "open_dt")
    Timestamp open_dt;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "open_reas_txt")
    String open_reas_txt;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "reval_flag")
    Boolean reval_flag;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "sid")
    String sid;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "subj_dp_code")
    String subj_dp_code;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "subj_sk")
    String subj_sk;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "syntacct_sk")
    String syntacct_sk;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "syntacct_dp_code")
    String syntacct_dp_code;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "tgt_purp_sk")
    String tgt_purp_sk;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "tgt_purp_dp_code")
    String tgt_purp_dp_code;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "acct_dp_code")
    String acct_dp_code;

    @PartitionField
    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "num", function = "substring(%s, 1, 3)")
    String acct_1_3_cd;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "num", function = "substring(%s, 4, 2)")
    String acct_4_5_cd;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "num", function = "substring(%s, 1, 5)")
    String acct_1_5_cd;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "num", function = "substring(%s, 6, 3)")
    String acct_6_8_cd;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "num", function = "substring(%s, 9, 1)")
    String acct_9_cd;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "num", function = "substring(%s, 10, 4)")
    String acct_10_13_cd;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "num", function = "substring(%s, 14, 7)")
    String acct_14_20_cd;

    @SourceTableField(sourceTable = ReplEssAcctTable.class, sourceFieldName = "src_ctl_validfrom", function = "substring(%s, 1, 10)")
    String begin_dt;

    //TODO добавить поддержку системных полей
    String user_name;
    Timestamp create_date;
    Long ctl_loading;
    Timestamp ctl_validfrom;
}