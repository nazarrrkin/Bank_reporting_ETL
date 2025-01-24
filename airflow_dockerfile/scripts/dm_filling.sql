/*
--дз
SELECT
	a."ACCOUNT_RK" AS debit_account_rk,
    ac."ACCOUNT_RK" AS credit_account_rk,
    SUM(p."DEBET_ACCOUNT_RK") AS total_debit,
    SUM(p."CREDIT_ACCOUNT_RK") AS total_credit
FROM "DS".ft_posting_f p
JOIN "DS".md_account_d a on p."DEBET_ACCOUNT_RK" = a."ACCOUNT_RK"
JOIN "DS".md_account_d ac on p."CREDIT_ACCOUNT_RK" = ac."ACCOUNT_RK"
WHERE "OPER_DATE" = '2018-01-15'
GROUP BY a."ACCOUNT_RK", ac."ACCOUNT_RK"
ORDER BY
    total_debit DESC,
    total_credit DESC
LIMIT 10;*/


create or replace procedure "DS".FILL_ACCOUNT_TURNOVER_F(i_OnDate date)
as $$
begin

    insert into "LOGS".LOGS("STATUS", "TIME")
    values (format('Calculating "DM".DM_ACCOUNT_TURNOVER_F for %s', i_OnDate), now());

    delete from "DM".DM_ACCOUNT_TURNOVER_F where "ON_DATE" = i_OnDate;

    with credit_data as (
        select
            p."CREDIT_ACCOUNT_RK" as "ACCOUNT_RK",
            sum(p."CREDIT_AMOUNT") as "CREDIT_AMOUNT",
            coalesce(SUM(p."CREDIT_AMOUNT") * max(m."REDUCED_COURCE"), sum(p."CREDIT_AMOUNT")) as "CREDIT_AMOUNT_RUB"
        from "DS".ft_posting_f p
        left join "DS".md_account_d a
            on p."CREDIT_ACCOUNT_RK" = a."ACCOUNT_RK"
        left join "DS".md_exchange_rate_d m
            on a."CURRENCY_RK" = m."CURRENCY_RK"
            and p."OPER_DATE" between m."DATA_ACTUAL_DATE" and m."DATA_ACTUAL_END_DATE"
        where p."OPER_DATE" = i_OnDate
        group by p."CREDIT_ACCOUNT_RK"
        order by "CREDIT_ACCOUNT_RK" asc
    ),

    debet_data as (
        select
            p."DEBET_ACCOUNT_RK" as "ACCOUNT_RK",
            SUM(p."DEBET_AMOUNT") as "DEBET_AMOUNT",
            coalesce(sum(p."DEBET_AMOUNT") * max(m."REDUCED_COURCE"), sum(p."DEBET_AMOUNT")) as "DEBET_AMOUNT_RUB"
        from "DS".ft_posting_f p
        left join "DS".md_account_d a
            on p."DEBET_ACCOUNT_RK" = a."ACCOUNT_RK"
        left join "DS".md_exchange_rate_d m
            on b."CURRENCY_RK" = m."CURRENCY_RK"
            and p."OPER_DATE" between m."DATA_ACTUAL_DATE" and m."DATA_ACTUAL_END_DATE"
        where p."OPER_DATE" = i_OnDate
        group by p."DEBET_ACCOUNT_RK"
        order by "DEBET_ACCOUNT_RK" asc
    )

    insert into "DM".DM_ACCOUNT_TURNOVER_F("ON_DATE", "ACCOUNT_RK", "CREDIT_AMOUNT", "CREDIT_AMOUNT_RUB", "DEBET_AMOUNT", "DEBET_AMOUNT_RUB")
    select
        i_OnDate as "ON_DATE",
        coalesce(c."ACCOUNT_RK", d."ACCOUNT_RK"),
        c."CREDIT_AMOUNT",
        c."CREDIT_AMOUNT_RUB",
        d."DEBET_AMOUNT",
        d."DEBET_AMOUNT_RUB"
    from credit_data c
    full join debet_data d on c."ACCOUNT_RK" = d."ACCOUNT_RK";

    insert into "LOGS".LOGS("STATUS", "TIME")
    values (format('"DM".DM_ACCOUNT_TURNOVER_F for %s has been calculated', i_OnDate), now());

end;
$$ language plpgsql;

do $$
declare
    start_date date := '2018-01-31';
begin
    while start_date <= '2018-01-31' loop
        call "DS".FILL_ACCOUNT_TURNOVER_F(start_date);
        start_date := start_date + interval '1 day';
    end loop;
end;
$$ language plpgsql;


insert into "DM".DM_ACCOUNT_BALANCE_F
select
    '2017-12-31' as "ON_DATE",
    b."ACCOUNT_RK",
    b."BALANCE_OUT",
    coalesce(b."BALANCE_OUT" * e."REDUCED_COURCE", b."BALANCE_OUT") as "BALANCE_OUT"
from "DS".FT_BALANCE_F b
left join "DS".MD_EXCHANGE_RATE_D e
    on b."CURRENCY_RK" = e."CURRENCY_RK"
    and e."DATA_ACTUAL_DATE" <= '2017-12-31' AND e."DATA_ACTUAL_END_DATE" >= '2017-12-31';


create or replace procedure "DS".FILL_ACCOUNT_BALANCE_F(i_OnDate date)
as $$
begin

    insert into "LOGS".LOGS("STATUS", "TIME")
    values (format('Calculating "DM".DM_ACCOUNT_BALANCE_F for %s', i_OnDate), now());

    delete from "DM".DM_ACCOUNT_BALANCE_F where "ON_DATE" = i_OnDate;

    insert into "DM".DM_ACCOUNT_BALANCE_F
    select i_OnDate as "ON_DATE",
    a."ACCOUNT_RK",
    case
        when a."CHAR_TYPE" = 'А' then
            coalesce(b."BALANCE_OUT", 0) + coalesce(t."DEBET_AMOUNT", 0) - coalesce(t."CREDIT_AMOUNT", 0)
        when a."CHAR_TYPE" = 'П' then
            coalesce(b."BALANCE_OUT", 0) - coalesce(t."DEBET_AMOUNT", 0) + coalesce(t."CREDIT_AMOUNT", 0)
    end as "BALANCE_OUT",
    case
        when a."CHAR_TYPE" = 'А' then
            coalesce(b."BALANCE_OUT", 0) * coalesce(e."REDUCED_COURCE", 1) + coalesce(t."DEBET_AMOUNT_RUB", 0) - coalesce(t."CREDIT_AMOUNT_RUB", 0)
        when a."CHAR_TYPE" = 'П' then
            coalesce(b."BALANCE_OUT", 0) * coalesce(e."REDUCED_COURCE", 1) - coalesce(t."DEBET_AMOUNT_RUB", 0) + coalesce(t."CREDIT_AMOUNT_RUB", 0)
    end as "BALANCE_OUT_RUB"
    from "DS".MD_ACCOUNT_D a
    left join "DS".FT_BALANCE_F b
        on a."ACCOUNT_RK" = b."ACCOUNT_RK"
        and "ON_DATE" = i_OnDate - interval '1 day'
    left join "DM".DM_ACCOUNT_TURNOVER_F t
        on a."ACCOUNT_RK" = t."ACCOUNT_RK"
        and t."ON_DATE" = i_OnDate
    left join "DS".MD_EXCHANGE_RATE_D e
        on a."CURRENCY_RK" = e."CURRENCY_RK"
        and i_OnDate between e."DATA_ACTUAL_DATE" and e."DATA_ACTUAL_END_DATE"
    where i_OnDate between a."DATA_ACTUAL_DATE" and a."DATA_ACTUAL_END_DATE";

    insert into "LOGS".LOGS("STATUS", "TIME")
    values (format('"DM".DM_ACCOUNT_BALANCE_F for %s has been calculated', i_OnDate), now());

end;
$$ language plpgsql;


do $$
declare
    start_date date := '2018-01-01';
begin
    while start_date <= '2018-01-31' loop
        call "DS".fill_account_balance_f(start_date);
        start_date := start_date + interval '1 day';
    end loop;
end;
$$ language plpgsql;


create or replace procedure "DM".FILL_F101_ROUND_F(i_OnDate date)
as $$
declare
    start_date date;
    end_date date;
begin
    start_date := date_trunc('month', i_OnDate) - interval '1 month';
    end_date := i_OnDate - interval '1 day';

    insert into "LOGS".LOGS("STATUS", "TIME")
    values (format('Calculating "DM".DM_F101_ROUND_F from %s to %s', start_date, end_date), now());

    delete from "DM".DM_F101_ROUND_F where "FROM_DATE" = start_date and "TO_DATE" = end_date;

    insert into "DM".DM_F101_ROUND_F(
        "FROM_DATE", "TO_DATE", "CHAPTER", "LEDGER_ACCOUNT", "CHARACTERISTIC",
        "BALANCE_IN_RUB", "R_BALANCE_IN_RUB", "BALANCE_IN_VAL", "R_BALANCE_IN_VAL", "BALANCE_IN_TOTAL", "R_BALANCE_IN_TOTAL",
        "TURN_DEB_RUB",  "R_TURN_DEB_RUB", "TURN_DEB_VAL", "R_TURN_DEB_VAL", "TURN_DEB_TOTAL","R_TURN_DEB_TOTAL",
        "TURN_CRE_RUB", "R_TURN_CRE_RUB", "TURN_CRE_VAL", "R_TURN_CRE_VAL", "TURN_CRE_TOTAL", "R_TURN_CRE_TOTAL",
        "BALANCE_OUT_RUB", "R_BALANCE_OUT_RUB", "BALANCE_OUT_VAL", "R_BALANCE_OUT_VAL", "BALANCE_OUT_TOTAL", "R_BALANCE_OUT_TOTAL"
    )
    select
        start_date as "FROM_DATE",
        end_date as "TO_DATE",
        l."CHAPTER",
        substr(a."ACCOUNT_NUMBER", 1, 5) as "LEDGER_ACCOUNT",
        a."CHAR_TYPE" as "CHARACTERISTIC",
        sum(case when a."CURRENCY_CODE" = '810' or a."CURRENCY_CODE" = '643' then b1."BALANCE_OUT_RUB" else 0 end) as "BALANCE_IN_RUB",
        sum(case when a."CURRENCY_CODE" = '810' or a."CURRENCY_CODE" = '643' then b1."BALANCE_OUT_RUB" else 0 end) / 1000 as "R_BALANCE_IN_RUB",
        sum(case when a."CURRENCY_CODE" != '810' and a."CURRENCY_CODE" != '643' then b1."BALANCE_OUT_RUB" else 0 end) as "BALANCE_IN_VAL",
        sum(case when a."CURRENCY_CODE" != '810' and a."CURRENCY_CODE" != '643' then b1."BALANCE_OUT_RUB" else 0 end) / 1000 as "R_BALANCE_IN_VAL",
        sum(b1."BALANCE_OUT_RUB") as "BALANCE_IN_TOTAL",
        sum(b1."BALANCE_OUT_RUB") / 1000 as "R_BALANCE_IN_TOTAL",
        sum(case when a."CURRENCY_CODE" = '810' or a."CURRENCY_CODE" = '643' then t."DEBET_AMOUNT_RUB" else 0 end) as "TURN_DEB_RUB",
        sum(case when a."CURRENCY_CODE" = '810' or a."CURRENCY_CODE" = '643' then t."DEBET_AMOUNT_RUB" else 0 end) / 1000 as "R_TURN_DEB_RUB",
        sum(case when a."CURRENCY_CODE" != '810' and a."CURRENCY_CODE" != '643' then t."DEBET_AMOUNT_RUB" else 0 end) as "TURN_DEB_VAL",
        sum(case when a."CURRENCY_CODE" != '810' and a."CURRENCY_CODE" != '643' then t."DEBET_AMOUNT_RUB" else 0 end) / 1000 as "R_TURN_DEB_VAL",
        sum(t."DEBET_AMOUNT_RUB") as "TURN_DEB_TOTAL",
        sum(t."DEBET_AMOUNT_RUB") / 1000 as "R_TURN_DEB_TOTAL",
        sum(case when a."CURRENCY_CODE" = '810' or a."CURRENCY_CODE" = '643' then t."CREDIT_AMOUNT_RUB" else 0 end) as "TURN_CRE_RUB",
        sum(case when a."CURRENCY_CODE" = '810' or a."CURRENCY_CODE" = '643' then t."CREDIT_AMOUNT_RUB" else 0 end) / 1000 as "R_TURN_CRE_RUB",
        sum(case when a."CURRENCY_CODE" != '810' and a."CURRENCY_CODE" != '643' then t."CREDIT_AMOUNT_RUB" else 0 end) as "TURN_CRE_VAL",
        sum(case when a."CURRENCY_CODE" != '810' and a."CURRENCY_CODE" != '643' then t."CREDIT_AMOUNT_RUB" else 0 end) / 1000 as "R_TURN_CRE_VAL",
        sum(t."CREDIT_AMOUNT_RUB") as "TURN_CRE_TOTAL",
        sum(t."CREDIT_AMOUNT_RUB") / 1000 as "R_TURN_CRE_TOTAL",
        sum(case when a."CURRENCY_CODE" = '810' or a."CURRENCY_CODE" = '643' then b2."BALANCE_OUT_RUB" else 0 end) as "BALANCE_OUT_RUB",
        sum(case when a."CURRENCY_CODE" = '810' or a."CURRENCY_CODE" = '643' then b2."BALANCE_OUT_RUB" else 0 end) / 1000 as "R_BALANCE_OUT_RUB",
        sum(case when a."CURRENCY_CODE" != '810' and a."CURRENCY_CODE" != '643' then b2."BALANCE_OUT_RUB" else 0 end) as "BALANCE_OUT_VAL",
        sum(case when a."CURRENCY_CODE" != '810' and a."CURRENCY_CODE" != '643' then b2."BALANCE_OUT_RUB" else 0 end) / 1000 as "R_BALANCE_OUT_VAL",
        sum(b2."BALANCE_OUT_RUB") as "BALANCE_OUT_TOTAL",
        sum(b2."BALANCE_OUT_RUB") / 1000 as "R_BALANCE_OUT_TOTAL"
    from "DS".MD_ACCOUNT_D a
    left join "DS".md_ledger_account_s l
        on substr(a."ACCOUNT_NUMBER", 1, 5)::integer = l."LEDGER_ACCOUNT"
    left join "DM".DM_ACCOUNT_BALANCE_F b1
        on a."ACCOUNT_RK" = b1."ACCOUNT_RK"
        and b1."ON_DATE" = start_date - interval '1 day'
    left join "DM".DM_ACCOUNT_TURNOVER_F t
        on a."ACCOUNT_RK" = t."ACCOUNT_RK"
        and t."ON_DATE" between start_date and end_date
    left join "DM".DM_ACCOUNT_BALANCE_F b2
        on a."ACCOUNT_RK" = b2."ACCOUNT_RK"
        and b2."ON_DATE" = end_date
    where
        a."DATA_ACTUAL_DATE" <= start_date
        and a."DATA_ACTUAL_END_DATE" >= end_date
    group by
        l."CHAPTER", substr(a."ACCOUNT_NUMBER", 1, 5), a."CHAR_TYPE";

    insert into "LOGS".LOGS("STATUS", "TIME")
    values (format('"DM".DM_F101_ROUND_F from %s to %s has been calculated', start_date, end_date), now());

end;
$$ language plpgsql;

call "DM".FILL_F101_ROUND_F('2018-02-01');

