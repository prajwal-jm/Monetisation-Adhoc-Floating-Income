-- Revenue from Debit Card Query

-- 1. Overall Metrics query

WITH pre_step1 as (
    SELECT *, 
    (case 
    when transaction_channel ='MANDATE' then 'BANK_TRANSFER'
    when transaction_channel is not null then transaction_channel
    when cbs_log_payee_particulars like '%TO ATM%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ATM %' then 'DEBIT_CARD' 
    when cbs_log_payee_particulars like '%TO ATM/%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM %' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM/%' then 'DEBIT_CARD'

    when cbs_log_payee_particulars like '%POS/%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%POS %' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%UPIOUT%' then 'UPI'
    when cbs_log_payer_particulars like '%UPI IN%' then 'UPI'
    when cbs_log_payer_particulars like '%UPI%' then 'UPI'

    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%IMPS%') then 'BANK_TRANSFER'
    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%NFT%')  then 'BANK_TRANSFER'
    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%IFN%') then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%IMPS%') then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%NFT%')  then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%IFN%') then 'BANK_TRANSFER'
    end) as transaction_channel1
    
    FROM transformations_db.jupiter_reconciliation_transaction_master as t1
    where 
        t1.updated_at = (select max(t2.updated_at) 
                         from transformations_db.jupiter_reconciliation_transaction_master as t2 
                         where t1.reconciliation_id = t2.reconciliation_id
                        )
)

, step1 as (
    SELECT reconciliation_id
    , customer_id
    , cbs_transaction_datetime
    , payment_transaction_datetime
    , transaction_amount
    , transaction_channel1
    
    , (case when cbs_log_payee_particulars like '%TO ATM%' then 'ATM'
            when cbs_log_payee_particulars like '%TO ECM%' then 'ECM'
            when cbs_log_payee_particulars like '%POS/%' then 'POS'
            else 'NA' end) as debit_card_use
    
    , (case when lower(coalesce(debit_card_txn_type, cbs_log_payee_particulars)) like '%atm%' then 'ATM' else 'Non_ATM' 
       end) as debit_card_txn_type
    
    , (case when debit_card_txn_scope is not null then debit_card_txn_scope
            when transactionparticulars2 like '%/IN' then 'DOMESTIC'
            when transactionparticulars2 not like '%/IN' then 'INTERNATIONAL' end) as debit_card_txn_scope

    FROM pre_step1 as t1
    WHERE t1.updated_at = (select max(t2.updated_at) 
                           from pre_step1 as t2 
                           where t1.reconciliation_id = t2.reconciliation_id)
    AND transaction_type = 'WITHDRAWAL'
    
    AND (
        (reconciliation_status IN ('RECONCILIATION_SUCCESSFUL') and transaction_channel='DEBIT_CARD') 
        or (reconciliation_status in ('RECONCILIATION_CBS_INITIATED') 
            and (cbs_log_payee_particulars like '%TO ATM%' or cbs_log_payee_particulars like '%TO ECM%' or cbs_log_payee_particulars like '%POS/%'))
        )
)

, step2 as (
    SELECT *
    , (case when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0037) -- these are interchange rates for revenue & expenses. refer to the confluence doc
         when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0146)
        end) as revenue

    , (case when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'ATM' then (transaction_amount*0.0009)
            when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'ATM' then (transaction_amount*0.0145)
            when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0011)
            when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.038)
        end) as expense

    FROM step1
)

, step3 as 
(select *, (case when revenue is null and expense is not null then (0-expense) else (revenue-expense) end) as gross_profit
FROM step2
)

, step4 as (
select 
count(distinct customer_id) as revenue_user,
count(distinct reconciliation_id) as revenue_txn_count,
sum(transaction_amount) as revenue_txn_amt,
sum(revenue) as revenue,
sum(expense) as expense,
sum(gross_profit) as gross_profit
from step3
)

select 
b.*,
a.revenue_user,
a.revenue_txn_count,
round(a.revenue_txn_amt,0) as revenue_txn_amt,
round(a.revenue, 0) as revenue,
round(a.expense,0) as expense,
round(a.gross_profit,0) as gross_profit
from step4 as a,
(select count(distinct customer_id) as total_user,
        count(distinct reconciliation_id) as total_txn_count,
        round(sum(transaction_amount), 0) as total_txn_amt,
        count(distinct case when transaction_type = 'WITHDRAWAL' then customer_id end) as debit_user,
        count(distinct case when transaction_type = 'WITHDRAWAL' then reconciliation_id end) as debit_txn_count,
        round(sum(case when transaction_type = 'WITHDRAWAL' then transaction_amount end),0) as debit_txn_amt
        from pre_step1
) as b



--------------------------------------------------------------------------------------------------------------------------------

-- 2. Revenue Vs Gross Profit query
-- here, most of the steps are same to the 1st query

WITH pre_step1 as (
    SELECT *, 
    (case 
    when transaction_channel ='MANDATE' then 'BANK_TRANSFER'
    when transaction_channel is not null then transaction_channel
    when cbs_log_payee_particulars like '%TO ATM%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ATM %' then 'DEBIT_CARD' 
    when cbs_log_payee_particulars like '%TO ATM/%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM %' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM/%' then 'DEBIT_CARD'

    when cbs_log_payee_particulars like '%POS/%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%POS %' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%UPIOUT%' then 'UPI'
    when cbs_log_payer_particulars like '%UPI IN%' then 'UPI'
    when cbs_log_payer_particulars like '%UPI%' then 'UPI'

    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%IMPS%') then 'BANK_TRANSFER'
    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%NFT%')  then 'BANK_TRANSFER'
    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%IFN%') then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%IMPS%') then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%NFT%')  then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%IFN%') then 'BANK_TRANSFER'
    end) as transaction_channel1
    
    FROM transformations_db.jupiter_reconciliation_transaction_master as t1
    where 
        t1.updated_at = (select max(t2.updated_at) 
                         from transformations_db.jupiter_reconciliation_transaction_master as t2 
                         where t1.reconciliation_id = t2.reconciliation_id
                        )
)

, step1 as (
    SELECT reconciliation_id
    , customer_id
    , cbs_transaction_datetime
    , payment_transaction_datetime
    , transaction_amount
    , transaction_channel1
    
    , (case when cbs_log_payee_particulars like '%TO ATM%' then 'ATM'
            when cbs_log_payee_particulars like '%TO ECM%' then 'ECM'
            when cbs_log_payee_particulars like '%POS/%' then 'POS'
            else 'NA' end) as debit_card_use
    
    , (case when lower(coalesce(debit_card_txn_type, cbs_log_payee_particulars)) like '%atm%' then 'ATM' else 'Non_ATM' 
       end) as debit_card_txn_type
    
    , (case when debit_card_txn_scope is not null then debit_card_txn_scope
            when transactionparticulars2 like '%/IN' then 'DOMESTIC'
            when transactionparticulars2 not like '%/IN' then 'INTERNATIONAL' end) as debit_card_txn_scope

    FROM pre_step1 as t1
    WHERE t1.updated_at = (select max(t2.updated_at) 
                           from pre_step1 as t2 
                           where t1.reconciliation_id = t2.reconciliation_id)
    AND transaction_type = 'WITHDRAWAL'
    
    AND (
        (reconciliation_status IN ('RECONCILIATION_SUCCESSFUL') and transaction_channel='DEBIT_CARD') 
        or (reconciliation_status in ('RECONCILIATION_CBS_INITIATED') 
            and (cbs_log_payee_particulars like '%TO ATM%' or cbs_log_payee_particulars like '%TO ECM%' or cbs_log_payee_particulars like '%POS/%'))
        )
)

, step2 as (
    SELECT *
    , (case when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0037)
         when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0146)
        end) as revenue

    , (case when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'ATM' then (transaction_amount*0.0009)
            when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'ATM' then (transaction_amount*0.0145)
            when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0011)
            when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.038)
        end) as expense

    FROM step1
)

, step3 as 
(select *, (case when revenue is null and expense is not null then (0-expense) else (revenue-expense) end) as gross_profit
FROM step2
)

select 
date_trunc('month', coalesce(cbs_transaction_datetime, payment_transaction_datetime)) as month_date,
-- date(coalesce(cbs_transaction_datetime, payment_transaction_datetime)) as date,
round(sum(revenue),0) as revenue,
round(sum(gross_profit),0) as gross_profit,
round(sum(expense),0) as expense
from step3
[[where date_trunc('month', coalesce(cbs_transaction_datetime, payment_transaction_datetime)) between {{start_date}} and {{end_date}}]]
group by 1
order by 1 desc

--------------------------------------------------------------------------------------------------------------------------------

-- 3. Monthly Transaction Table query

WITH pre_step1 as (
    SELECT *, 
    (case 
    when transaction_channel ='MANDATE' then 'BANK_TRANSFER'
    when transaction_channel is not null then transaction_channel
    when cbs_log_payee_particulars like '%TO ATM%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ATM %' then 'DEBIT_CARD' 
    when cbs_log_payee_particulars like '%TO ATM/%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM %' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM/%' then 'DEBIT_CARD'

    when cbs_log_payee_particulars like '%POS/%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%POS %' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%UPIOUT%' then 'UPI'
    when cbs_log_payer_particulars like '%UPI IN%' then 'UPI'
    when cbs_log_payer_particulars like '%UPI%' then 'UPI'

    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%IMPS%') then 'BANK_TRANSFER'
    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%NFT%')  then 'BANK_TRANSFER'
    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%IFN%') then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%IMPS%') then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%NFT%')  then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%IFN%') then 'BANK_TRANSFER'
    end) as transaction_channel1
    
    FROM transformations_db.jupiter_reconciliation_transaction_master as t1
    where 
        t1.updated_at = (select max(t2.updated_at) 
                         from transformations_db.jupiter_reconciliation_transaction_master as t2 
                         where t1.reconciliation_id = t2.reconciliation_id
                        )
)

, step1 as (
    SELECT reconciliation_id
    , customer_id
    , cbs_transaction_datetime
    , payment_transaction_datetime
    , transaction_amount
    , transaction_channel1
    
    , (case when cbs_log_payee_particulars like '%TO ATM%' then 'ATM'
            when cbs_log_payee_particulars like '%TO ECM%' then 'ECM'
            when cbs_log_payee_particulars like '%POS/%' then 'POS'
            else 'NA' end) as debit_card_use
    
    , (case when lower(coalesce(debit_card_txn_type, cbs_log_payee_particulars)) like '%atm%' then 'ATM' else 'Non_ATM' 
       end) as debit_card_txn_type
    
    , (case when debit_card_txn_scope is not null then debit_card_txn_scope
            when transactionparticulars2 like '%/IN' then 'DOMESTIC'
            when transactionparticulars2 not like '%/IN' then 'INTERNATIONAL' end) as debit_card_txn_scope

    FROM pre_step1 as t1
    WHERE t1.updated_at = (select max(t2.updated_at) 
                           from pre_step1 as t2 
                           where t1.reconciliation_id = t2.reconciliation_id)
    AND transaction_type = 'WITHDRAWAL'
    
    AND (
        (reconciliation_status IN ('RECONCILIATION_SUCCESSFUL') and transaction_channel='DEBIT_CARD') 
        or (reconciliation_status in ('RECONCILIATION_CBS_INITIATED') 
            and (cbs_log_payee_particulars like '%TO ATM%' or cbs_log_payee_particulars like '%TO ECM%' or cbs_log_payee_particulars like '%POS/%'))
        )
)

, step2 as (
    SELECT *
    , (case when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0037)
         when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0146)
        end) as revenue

    , (case when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'ATM' then (transaction_amount*0.0009)
            when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'ATM' then (transaction_amount*0.0145)
            when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0011)
            when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.038)
        end) as expense

    FROM step1
)

, step3 as 
(select *, (case when revenue is null and expense is not null then (0-expense) else (revenue-expense) end) as gross_profit
FROM step2
)


select 
a.month_date,
b.debit_txn_amt,
a.revenue_txn_amount,
a.revenue,
a.expense,
a.gross_profit

from (select date(date_trunc('month', coalesce(cbs_transaction_datetime, payment_transaction_datetime))) as month_date,
            round(sum(transaction_amount), 0) as revenue_txn_amount,
            round(sum(revenue), 0) as revenue,
            round(sum(expense), 0) as expense,
            round(sum(gross_profit), 0) as gross_profit
            from step3
            group by 1
    ) a
join (select date(date_trunc('month', coalesce(cbs_transaction_datetime, payment_transaction_datetime))) as month_date,
            round(sum(case when transaction_type = 'WITHDRAWAL' then transaction_amount end),0) as debit_txn_amt,
            count(distinct case when transaction_type = 'WITHDRAWAL' then reconciliation_id end) as debit_txn_count,
            count(distinct case when transaction_type = 'WITHDRAWAL' then customer_id end) as debit_user -- and transaction_type = 'WITHDRAWAL' 
            from pre_step1
            group by 1
    ) b
on a.month_date = b.month_date
order by 1 desc

--------------------------------------------------------------------------------------------------------------------------------

-- 4. Revenue Generating Users & Transactions query

WITH pre_step1 as (
    SELECT *, 
    (case 
    when transaction_channel ='MANDATE' then 'BANK_TRANSFER'
    when transaction_channel is not null then transaction_channel
    when cbs_log_payee_particulars like '%TO ATM%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ATM %' then 'DEBIT_CARD' 
    when cbs_log_payee_particulars like '%TO ATM/%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM %' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM/%' then 'DEBIT_CARD'

    when cbs_log_payee_particulars like '%POS/%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%POS %' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%UPIOUT%' then 'UPI'
    when cbs_log_payer_particulars like '%UPI IN%' then 'UPI'
    when cbs_log_payer_particulars like '%UPI%' then 'UPI'

    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%IMPS%') then 'BANK_TRANSFER'
    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%NFT%')  then 'BANK_TRANSFER'
    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%IFN%') then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%IMPS%') then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%NFT%')  then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%IFN%') then 'BANK_TRANSFER'
    end) as transaction_channel1
    
    FROM transformations_db.jupiter_reconciliation_transaction_master as t1
    where 
        t1.updated_at = (select max(t2.updated_at) 
                         from transformations_db.jupiter_reconciliation_transaction_master as t2 
                         where t1.reconciliation_id = t2.reconciliation_id
                        )
)

, step1 as (
    SELECT reconciliation_id
    , customer_id
    , cbs_transaction_datetime
    , payment_transaction_datetime
    , transaction_amount
    , transaction_channel1
    
    , (case when cbs_log_payee_particulars like '%TO ATM%' then 'ATM'
            when cbs_log_payee_particulars like '%TO ECM%' then 'ECM'
            when cbs_log_payee_particulars like '%POS/%' then 'POS'
            else 'NA' end) as debit_card_use
    
    , (case when lower(coalesce(debit_card_txn_type, cbs_log_payee_particulars)) like '%atm%' then 'ATM' else 'Non_ATM' 
       end) as debit_card_txn_type
    
    , (case when debit_card_txn_scope is not null then debit_card_txn_scope
            when transactionparticulars2 like '%/IN' then 'DOMESTIC'
            when transactionparticulars2 not like '%/IN' then 'INTERNATIONAL' end) as debit_card_txn_scope

    FROM pre_step1 as t1
    WHERE t1.updated_at = (select max(t2.updated_at) 
                           from pre_step1 as t2 
                           where t1.reconciliation_id = t2.reconciliation_id)
    AND transaction_type = 'WITHDRAWAL'
    
    AND (
        (reconciliation_status IN ('RECONCILIATION_SUCCESSFUL') and transaction_channel='DEBIT_CARD') 
        or (reconciliation_status in ('RECONCILIATION_CBS_INITIATED') 
            and (cbs_log_payee_particulars like '%TO ATM%' or cbs_log_payee_particulars like '%TO ECM%' or cbs_log_payee_particulars like '%POS/%'))
        )
)

, step2 as (
    SELECT *
    , (case when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0037)
         when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0146)
        end) as revenue

    , (case when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'ATM' then (transaction_amount*0.0009)
            when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'ATM' then (transaction_amount*0.0145)
            when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0011)
            when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.038)
        end) as expense

    FROM step1
)

, step3 as 
(select *, (case when revenue is null and expense is not null then (0-expense) else (revenue-expense) end) as gross_profit
FROM step2
)

select 
date_trunc('month', coalesce(cbs_transaction_datetime, payment_transaction_datetime)) as month_date,
-- date(coalesce(cbs_transaction_datetime, payment_transaction_datetime)) as date,
count(distinct case when debit_card_txn_scope in ('DOMESTIC', 'INTERNATIONAL') and debit_card_txn_type = 'Non_ATM' then customer_id end) as user_count,
count(distinct case when debit_card_txn_scope in ('DOMESTIC', 'INTERNATIONAL') and debit_card_txn_type = 'Non_ATM' then reconciliation_id end) as txn_count
from step3
[[where date_trunc('month', coalesce(cbs_transaction_datetime, payment_transaction_datetime)) between {{start_date}} and {{end_date}}]]
group by 1
order by 1 desc

--------------------------------------------------------------------------------------------------------------------------------

-- 5. (ATM, Non-ATM Split) and (Domestic, International Txns Split) and (Debit Card Usage Split) Query

WITH pre_step1 as (
    SELECT *, 
    (case 
    when transaction_channel ='MANDATE' then 'BANK_TRANSFER'
    when transaction_channel is not null then transaction_channel
    when cbs_log_payee_particulars like '%TO ATM%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ATM %' then 'DEBIT_CARD' 
    when cbs_log_payee_particulars like '%TO ATM/%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM %' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%TO ECM/%' then 'DEBIT_CARD'

    when cbs_log_payee_particulars like '%POS/%' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%POS %' then 'DEBIT_CARD'
    when cbs_log_payee_particulars like '%UPIOUT%' then 'UPI'
    when cbs_log_payer_particulars like '%UPI IN%' then 'UPI'
    when cbs_log_payer_particulars like '%UPI%' then 'UPI'

    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%IMPS%') then 'BANK_TRANSFER'
    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%NFT%')  then 'BANK_TRANSFER'
    when transaction_type='WITHDRAWAL' and cbs_log_payee_particulars like ('%IFN%') then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%IMPS%') then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%NFT%')  then 'BANK_TRANSFER'
    when transaction_type='DEPOSIT' and cbs_log_payer_particulars like ('%IFN%') then 'BANK_TRANSFER'
    end) as transaction_channel1
    
    FROM transformations_db.jupiter_reconciliation_transaction_master as t1
    where 
        t1.updated_at = (select max(t2.updated_at) 
                         from transformations_db.jupiter_reconciliation_transaction_master as t2 
                         where t1.reconciliation_id = t2.reconciliation_id
                        )
)

, step1 as (
    SELECT reconciliation_id
    , customer_id
    , cbs_transaction_datetime
    , payment_transaction_datetime
    , transaction_amount
    , transaction_channel1
    
    , (case when cbs_log_payee_particulars like '%TO ATM%' then 'ATM'
            when cbs_log_payee_particulars like '%TO ECM%' then 'ECM'
            when cbs_log_payee_particulars like '%POS/%' then 'POS'
            else 'NA' end) as debit_card_use
    
    , (case when lower(coalesce(debit_card_txn_type, cbs_log_payee_particulars)) like '%atm%' then 'ATM' else 'Non_ATM' 
       end) as debit_card_txn_type
    
    , (case when debit_card_txn_scope is not null then debit_card_txn_scope
            when transactionparticulars2 like '%/IN' then 'DOMESTIC'
            when transactionparticulars2 not like '%/IN' then 'INTERNATIONAL' end) as debit_card_txn_scope

    FROM pre_step1 as t1
    WHERE t1.updated_at = (select max(t2.updated_at) 
                           from pre_step1 as t2 
                           where t1.reconciliation_id = t2.reconciliation_id)
    AND transaction_type = 'WITHDRAWAL'
    
    AND (
        (reconciliation_status IN ('RECONCILIATION_SUCCESSFUL') and transaction_channel='DEBIT_CARD') 
        or (reconciliation_status in ('RECONCILIATION_CBS_INITIATED') 
            and (cbs_log_payee_particulars like '%TO ATM%' or cbs_log_payee_particulars like '%TO ECM%' or cbs_log_payee_particulars like '%POS/%'))
        )
)

, step2 as (
    SELECT *
    , (case when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0037)
         when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0146)
        end) as revenue

    , (case when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'ATM' then (transaction_amount*0.0009)
            when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'ATM' then (transaction_amount*0.0145)
            when debit_card_txn_scope = 'DOMESTIC' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.0011)
            when debit_card_txn_scope = 'INTERNATIONAL' and debit_card_txn_type = 'Non_ATM' then (transaction_amount*0.038)
        end) as expense

    FROM step1
)

, step3 as 
(select *, (case when revenue is null and expense is not null then (0-expense) else (revenue-expense) end) as gross_profit
FROM step2
)

select 
date_trunc('month', coalesce(cbs_transaction_datetime, payment_transaction_datetime)) as month_date,
debit_card_txn_type, -- (to get the split between ATM & Non-ATM Txns)
-- debit_card_txn_scope (to get the split between Domestic & International)
-- debit_card_use (dc usage split)
sum(gross_profit) as gross_profit
from step3
where debit_card_txn_scope is not null
[[and date_trunc('month', coalesce(cbs_transaction_datetime, payment_transaction_datetime)) between {{start_date}} and {{end_date}}]]
group by 1, 2
order by 1 desc

--------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------


