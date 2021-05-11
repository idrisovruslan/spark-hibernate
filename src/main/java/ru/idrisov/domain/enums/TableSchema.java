package ru.idrisov.domain.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum TableSchema {
    // DO NOT NAME DEFAULT SCHEMA AS DEFAULT!
    DEFAULT_SCHEMA("default_schema"),
    CUSTOM_FIN_FSO_STG("custom_fin_fso_stg"),
    CUSTOM_FIN_FSO_TMD_STG("custom_fin_fso_tmd_stg"),
    CUSTOM_FIN_FSO_TMD("custom_fin_fso_tmd"),
    CUSTOM_FIN_FSO_DMREP("custom_fin_fso_dmrep"),
    CUSTOM_FIN_FSO_DMDET("custom_fin_fso_dmdet"),
    CUSTOM_FIN_FSO_HIST("custom_fin_fso_hist"),
    ODS_068_000000_IPCHDP("ods_068_000000_ipchdp"),
    CUSTOM_FIN_FSO_DMDS("custom_fin_fso_dmds"),
    CUSTOM_FIN_FSO_DMDQ_DMDS("custom_fin_fso_dmdqdmds"),
    CUSTOM_FIN_FSO_DMDQ_DMDET("custom_fin_fso_dmdqdmdet"),
    CUSTOM_FIN_FSO_DMDQ_DMREP("custom_fin_fso_dmdqdmrep");

    private final String schemaName;
}
