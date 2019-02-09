select
   cc.segment1
  ,cc.segment1_desc
  ,cc.segment2
  ,cc.segment2_desc
  ,prd.product_number
  ,prd.format
  ,prd.language
  ,prd.product_number || '-' || prd.format || '-' || prd.language product_number_concat
  ,prd.title_abbreviation
  ,sd.bill_to_party_id
  ,sd.salesrep_id
  ,sr.salesrep_name as "ebs_rep"
  ,pa.party_name
  ,pa.party_number partynumber_aka_registryid
  ,cb.cust_account_id bill_to_account_id
  ,cb.account_name bill_to_account_name
  ,cb.account_number bill_to_account_number
  ,cb.parent_number parentnumber_aka_parentregid
  ,cb.parent_name bill_to_parent_name
  ,cs.cust_account_id sold_to_account_id
  ,cs.account_name sold_to_account_name
  ,cs.account_number sold_to_account_number
  ,dt.fiscal_yyyymm
  ,dt.date_field "Transaction Date"
  ,CASE 
     WHEN dt.date_field <= (sysdate-1)
       THEN 1 -- Recognized Revenue
       ELSE 2 -- Deferred Revenue
   END "Revenue Type ID"
  ,sd.order_header_id
  ,sum(sd.line_amount) as "Amount"
    from sales_detail sd
    inner join gl_code_combos cc
      on sd.code_combination_id=cc.code_combination_id
    inner join dim_dates_v dt
      on sd.gl_date_key=dt.date_key
    inner join products prd
      on sd.inventory_item_id=prd.inventory_item_id
    left outer join parties pa
      on sd.bill_to_party_id=pa.party_id
    left outer join customers_bill_to cb
      on sd.bill_to_customer_id=cb.cust_account_id
    left outer join customers_sold_to cs
      on sd.sold_to_customer_id=cs.cust_account_id
	left join salesreps sr
	  on sd.salesrep_id = sr.salesrep_id
  where dt.fiscal_yyyymm >= '201401' --'201703'
	and dt.fiscal_yyyymm <= '205001'
    --and rownum < 10 
  and cc.segment1='01'
  and cc.segment2='610'
 group by 
   cc.segment1
  ,cc.segment1_desc
  ,cc.segment2
  ,cc.segment2_desc
  ,prd.product_number
  ,prd.format
  ,prd.language
  ,prd.title_abbreviation
  ,sd.bill_to_party_id
  ,pa.party_name
  ,pa.party_number
  ,cb.cust_account_id
  ,cb.account_name
  ,cb.account_number
  ,cb.parent_number
  ,cb.parent_name
  ,cs.cust_account_id
  ,cs.account_name
  ,cs.account_number
  ,dt.fiscal_yyyymm
  ,dt.date_field
  ,sd.order_header_id
  ,sd.salesrep_id
  ,sr.salesrep_name
 order by
  dt.date_field desc