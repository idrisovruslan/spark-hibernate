package ru.idrisov.domain.entitys.stg;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.universal_loader.annotations.EntitySpark;
import ru.idrisov.universal_loader.annotations.PartitionField;
import ru.idrisov.universal_loader.entitys.TableSpark;

import java.sql.Timestamp;

@Component
@FieldDefaults(level= AccessLevel.PUBLIC)
@EntitySpark(tableSchema = "custom_fin_fso_tmd_stg", tableName = "repl_ess_acct", filling = false)
public class ReplEssAcctTable implements TableSpark {

    Long src_ctl_loading;
    Timestamp src_ctl_validfrom;
    //TODO добавить потдержку DecimalType
    createDecimal src_ctl_csn;
    String acct_sk;
    String active;
    String acct_purp_sk;
    Timestamp close_dt;
    String close_reas_txt;
    Boolean cnsld_flag;
    String crncy_sk;
    String depo_subj_sk;
    String div_sk;
    Boolean escrow_flag;
    String frsymb_sk;
    String name;
    Timestamp ntf_tax_close_dt;
    Timestamp ntf_tax_open_dt;
    String num;
    Timestamp open_dt;
    String open_reas_txt;
    Boolean reval_flag;
    String sid;
    String subj_sk;
    String syntacct_sk;
    String tgt_purp_sk;
    String acct_dp_code;
    Boolean dirty_flag;
    String gold_sid;
    String instance_code;
    String crncy_dp_code;
    String depo_subj_dp_code;
    String frsymb_dp_code;
    String subj_dp_code;
    String syntacct_dp_code;
    String tgt_purp_dp_code;
    Timestamp create_date;
    String user_name;
    Long ctl_loading;
    Timestamp ctl_validfrom;

    @PartitionField
    String ctl_loading_date;

}