select
    `t`.`num_contr` AS `IdBoleta`,
    `t`.`num_trade` AS `IdTrade`,
    `c`.`dat_trade` AS `Data Operacao`,
    rtrim(`resp`.`nom_user`) AS `Responsavel`,
    `t`.`dat_initi` AS `Data Inicio`,
    `t`.`dat_final` AS `Data Final`,
    `t`.`dat_payme` AS `Data Pagamento`,
    (case
        when (`t`.`val_volum` < 0) then 'Venda'
        else 'Compra'
    end) AS `C/V`,
    rtrim(`grp`.`nom_count_group`) AS `Grupo`,
    rtrim(`unit`.`nom_count`) AS `Unidade`,
    rtrim(`region`.`cod_regio`) AS `Submercado`,
    rtrim(`et`.`nom_energ_type`) AS `Tipo de Energia`,
    `t`.`val_volum` AS `Volume`,
    ifnull(`tExt`.`val_volum_sazo`, 0) AS `Volume Sazo`,
    `t`.`val_volum_adjus` AS `Volume Ajustado`,
    `c`.`val_min_volum` AS `Flex Min`,
    `c`.`val_max_volum` AS `Flex Max`,
    `c`.`val_seaso_min` AS `Sazo Min`,
    `c`.`val_seaso_max` AS `Sazo Max`,
    rtrim(`pt`.`nom_price_type`) AS `Preco Tipo`,
    `t`.`val_price` AS `Preco`,
    `t`.`val_price_adjus` AS `Preco Ajustado`,
    rtrim(`dt`.`nom_distr_ccee`) AS `Tipo de Distribuicao`,
    rtrim(`rt`.`nom_regis_type`) AS `Registro`,
    rtrim(`gt`.`nom_warra_type`) AS `Garantia`,
    rtrim(`ind`.`nom_index`) AS `Indice Reaj`,
    `c`.`dat_index_base` AS `Data Base Reaj`,
    `c`.`dat_index_first_adjus` AS `Data Primeiro Reaj`,
    `tExt`.`data_update` AS `Data Ult Alteracao`,
    rtrim(`lastUser`.`nom_user`) AS `Usuário Ult Alteracao`,
    rtrim(`ext`.`num_contr_bbce`) AS `Cod BBCE`,
    rtrim(`cart`.`nom_carteira`) AS `Carteira`,
    ifnull(`pld`.`val_pld`, 0) AS `PLD`,
    0.0 AS `valor_forward`,
    CONCAT( 
        convert( rtrim(`region`.`cod_regio`) using utf8), ' ',
        convert( rtrim(`et`.`nom_energ_bbce`) using utf8), ' ',
        UPPER(CASE 
            WHEN (( (YEAR(c.dat_final) - YEAR(c.dat_initi ) ) * 12) + (MONTH(c.dat_final) - (MONTH(c.dat_initi)) ) + 1) = 1  THEN CONCAT('MEN ', DATE_FORMAT(c.dat_initi,'%b/%y'))
            WHEN (( (YEAR(c.dat_final) - YEAR(c.dat_initi ) ) * 12) + (MONTH(c.dat_final) - (MONTH(c.dat_initi)) ) + 1) = 2  THEN CONCAT('BIM ', DATE_FORMAT(c.dat_initi,'%b/%y'), ' ', DATE_FORMAT(c.dat_final,'%b/%y'))
            WHEN (( (YEAR(c.dat_final) - YEAR(c.dat_initi ) ) * 12) + (MONTH(c.dat_final) - (MONTH(c.dat_initi)) ) + 1) = 3  THEN CONCAT('TRI ', DATE_FORMAT(c.dat_initi,'%b/%y'), ' ', DATE_FORMAT(c.dat_final,'%b/%y'))
            WHEN (( (YEAR(c.dat_final) - YEAR(c.dat_initi ) ) * 12) + (MONTH(c.dat_final) - (MONTH(c.dat_initi)) ) + 1) = 6  THEN CONCAT('SEM ', DATE_FORMAT(c.dat_initi,'%b/%y'), ' ', DATE_FORMAT(c.dat_final,'%b/%y'))
            WHEN (( (YEAR(c.dat_final) - YEAR(c.dat_initi ) ) * 12) + (MONTH(c.dat_final) - (MONTH(c.dat_initi)) ) + 1) = 12 THEN CONCAT('ANU ', DATE_FORMAT(c.dat_initi,'%b/%y'), ' ', DATE_FORMAT(c.dat_final,'%b/%y'))
            ELSE CONCAT('OTR ', DATE_FORMAT(c.dat_initi,'%b/%y'), ' ', DATE_FORMAT(c.dat_final,'%b/%y'))
        END), ' - ',
        convert( rtrim(`pt`.`nom_price_type_bbce`) using utf8)
    )
    as `Produto BBCE`
from
    ((((((((((((((((`tbl_trade` `t`
join `tbl_trade_extension` `tExt` on
    ((`t`.`num_trade` = `tExt`.`num_trade_id`)))
join `tbl_contract` `c` on
    ((`c`.`num_contr` = `t`.`num_contr`)))
join `tbl_contract_extension` `ext` on
    ((`ext`.`num_contract_id` = `c`.`num_contr`)))
join `tbl_user` `resp` on
    ((`c`.`num_user` = `resp`.`num_user`)))
join `tbl_user` `lastUser` on
    ((`tExt`.`num_user_update` = `lastUser`.`num_user`)))
join `tbl_counterparty_group` `grp` on
    ((`grp`.`num_count_group` = `c`.`num_count_group`)))
join `tbl_counterparty` `unit` on
    ((`unit`.`num_count` = `c`.`num_count`)))
join `tbl_region` `region` on
    ((`region`.`num_regio` = `c`.`num_regio`)))
join `tbl_energy_type` `et` on
    ((`et`.`num_energ_type` = `c`.`num_energ_type`)))
join `tbl_price_type` `pt` on
    ((`pt`.`num_price_type` = `c`.`num_price_type`)))
join `tbl_distribution_type` `dt` on
    ((`dt`.`num_distr_type` = `c`.`num_distr_type`)))
join `tbl_registration_type` `rt` on
    ((`rt`.`num_regis_type` = `c`.`num_regis_type`)))
join `tbl_warrant_type` `gt` on
    ((`gt`.`num_warra_type` = `c`.`num_warra_type`)))
join `tbl_index` `ind` on
    ((`ind`.`num_index` = `c`.`num_index`)))
left join `tbl_carteira` `cart` on
    ((`cart`.`num_carteira` = `c`.`num_carteira`)))
left join `tbl_pld` `pld` on
    (((`pld`.`dat_initi` = `t`.`dat_initi`)
    and (`pld`.`num_regio` = `c`.`num_regio`))))
where
    (`ext`.`ind_cancel` = FALSE)
    and (`t`.`ind_cance` = FALSE)
;