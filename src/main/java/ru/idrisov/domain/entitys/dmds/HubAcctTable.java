package ru.idrisov.domain.entitys.dmds;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Component;
import ru.idrisov.universal_loader.annotations.EntitySpark;
import ru.idrisov.universal_loader.annotations.PartitionField;
import ru.idrisov.universal_loader.entitys.TableSpark;

import java.sql.Timestamp;

@Component
@FieldDefaults(level = AccessLevel.PUBLIC)
@EntitySpark(tableSchema = "custom_fin_fso_dmds", tableName = "hub_acct", filling = false)
public class HubAcctTable implements TableSpark {

    String acct_sk;
    Boolean cnsld_flag;
    String crncy_sk;
    String subj_depo_sk;
    Boolean escrow_flag;
    String frsymb_sk;
    String name;
    Timestamp ntf_tax_open_dt;
    String num;
    Timestamp open_dt;
    String open_reas_txt;
    Boolean reval_flag;
    String sid;
    String subj_sk;
    String syntacct_sk;
    String tgt_purp_sk;

    @PartitionField
    String acct_1_3_cd;

    String acct_4_5_cd;
    String acct_1_5_cd;
    String acct_6_8_cd;
    String acct_9_cd;
    String acct_10_13_cd;
    String acct_14_20_cd;
    String begin_dt;
    String acct_dp_code;
    String crncy_dp_code;
    String depo_subj_dp_code;
    Boolean dirty_flag;
    String frsymb_dp_code;
    String gold_sid;
    String instance_code;
    String subj_dp_code;
    String syntacct_dp_code;
    String tgt_purp_dp_code;
    Timestamp create_date;
    String user_name;
    Long ctl_loading;
    Timestamp ctl_validfrom;

}