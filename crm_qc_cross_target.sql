with base as (
    SELECT
      external_id,
      min(date) date,
      max(campaign_name) campaign_name,
      max(campaign_variant_name) campaign_variant_name
    FROM
      `tlb-data-prod.data_platform.fct_campaign_user_event` a
    LEFT JOIN
      `tlb-data-prod.data_platform.dim_campaign`
    USING(campaign_id)
    LEFT JOIN
      `tlb-data-prod.data_platform.dim_campaign_event_type`
    USING(event_type_id)
    LEFT JOIN
      `tlb-data-prod.data_platform.dim_campaign_variant`
    USING(campaign_variant_id)
    LEFT JOIN
      `tlb-data-prod.data_platform.dim_campaign_message`
    USING(campaign_message_id)
    WHERE
      campaign_name in
      (
      'DMA_AUT_240711_KW_TAL_AH_QC-Cross-Vertical_Voucher_CAN'
      )
    AND
      date between date('2024-07-11') and date('2024-08-09')
    AND
      event_type = "canvas_entry"
    GROUP BY 1
)
,orders AS (
    SELECT
      external_id,
      date,
      campaign_name,
      campaign_variant_name,
      count(distinct base.external_id) as base_users,
      count(DISTINCT ord.account_id) order_users,
      count(DISTINCT IF(ord.order_date between date and date + 90, ord.order_id, null)) platform_orders,
      count(DISTINCT IF(ord.order_date between date - (90+1) and date - 1, ord.order_id, null)) platform_orders_pre,
      count(DISTINCT IF(ord.order_date between date and date + 28, ord.order_id, null)) platform_28d_orders,
      count(DISTINCT IF(ord.order_date between date - 29 and date - 1, ord.order_id, null)) platform_28d_orders_pre,
      count(DISTINCT IF(ord.is_non_food and ord.order_date between date and date + 90, ord.order_id, null)) qc_orders,
      count(DISTINCT IF(ord.is_non_food and ord.order_date between date - (90+1) and date - 1, ord.order_id, null)) qc_orders_pre,
      count(distinct case when ord.is_non_food and ord.order_date between date and date+28 then ord.order_id end) as qc_28d_orders,
      count(DISTINCT IF(ord.is_darkstore and ord.order_date between date and date + 90, ord.order_id, null)) tmart_orders,
      count(DISTINCT IF(ord.is_darkstore = False and ord.is_non_food = True and ord.order_date between date and date + 90, ord.order_id, null)) ls_orders,
      sum(IF(ord.order_date between date and date + 90, ord.gmv_amount_eur, null)) sum_gmv_eur,
      sum(IF(ord.order_date between date - (90+1) and date - 1, ord.gmv_amount_eur, null)) sum_gmv_eur_pre,
      sum(IF(ord.is_non_food and ord.order_date between date and date + 90, ord.gmv_amount_eur, null)) sum_qc_gmv_eur,
      sum(IF(ord.is_non_food and ord.order_date between date - (90+1) and date - 1, ord.gmv_amount_eur, null)) sum_qc_gmv_eur_pre,
      sum(IF(ord.is_darkstore and ord.order_date between date and date + 90, ord.gmv_amount_eur, null)) sum_tmart_gmv_eur,
      sum(IF(ord.is_darkstore and ord.order_date between date - (90+1) and date - 1, ord.gmv_amount_eur, null)) sum_tmart_gmv_eur_pre,
      sum(IF(ord.is_darkstore = FALSE and ord.is_non_food = True and ord.order_date between date - (90+1) and date - 1, ord.gmv_amount_eur, null)) sum_ls_gmv_eur_pre,
      sum(IF(ord.is_darkstore = FALSE and ord.is_non_food = True and ord.order_date between date and date + 90, ord.gmv_amount_eur, null)) sum_ls_gmv_eur,
      sum(IF(ord.order_date between date and date + 90, coalesce(IF(act_egpo_eur > 0, act_egpo_eur, null), est_egpo_eur), null)) platform_egpo_eur,
      sum(IF(ord.order_date between date - (90+1) and date - 1, coalesce(IF(act_egpo_eur > 0, act_egpo_eur, null), est_egpo_eur), null)) platform_egpo_eur_pre,
      sum(if(is_non_food_acquisition,1,0)) as qc_acquisition,
      sum(if(is_darkstore_acquisition,1,0)) as tmart_acquisition

    FROM
      base
    LEFT JOIN
      `tlb-data-prod.data_platform.fct_order_info` ord
    ON
      external_id = CAST(account_id as string)
    AND
      is_successful
    LEFT JOIN
      `tlb-data-prod.data_platform.dim_vendor_info`
    USING(chain_id, vendor_id)
    LEFT JOIN
      `tlb-data-prod.data_platform.fct_order_egpo`
    USING(order_id)
    GROUP BY
    1,2,3,4
)
select
campaign_name
,campaign_variant_name
,sum(base_users) as base_users
,sum(order_users) as order_users
,count(distinct case when qc_28d_orders >=4 then external_id end) as qc_mhu
,sum(platform_orders) as platform_orders
,avg(platform_orders) as platform_order_freq_90d
,sum(qc_orders) as qc_orders
,sum(qc_orders_pre) as qc_orders_pre
,avg(qc_orders) as qc_orders_freq_90d
,sum(tmart_orders) as tmart_orders
,sum(ls_orders) as ls_orders
,sum(sum_gmv_eur) as sum_gmv_eur
,sum(sum_qc_gmv_eur) as sum_qc_gmv_eur
,sum(sum_qc_gmv_eur_pre) as sum_qc_gmv_eur_pre
,sum(sum_tmart_gmv_eur) as sum_tmart_gmv_eur
,sum(sum_tmart_gmv_eur_pre) as sum_tmart_gmv_eur_pre
,sum(sum_ls_gmv_eur_pre) as sum_ls_gmv_eur_pre
,sum(sum_ls_gmv_eur) as sum_ls_gmv_eur
,sum(platform_egpo_eur) as platform_egpo_eur
,sum(platform_egpo_eur_pre) as platform_egpo_eur_pre
,sum(qc_acquisition) as qc_acquisition
,sum(tmart_acquisition) as tmart_acquisition
from orders
group by
1,2
order by
1,2

