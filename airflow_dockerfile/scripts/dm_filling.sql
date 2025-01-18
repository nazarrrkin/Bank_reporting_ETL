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


create or replace procedure "DS".fill_account_turnover_f(i_OnDate date)
as $$
begin
    with credit_data as (
        select
            p."CREDIT_ACCOUNT_RK" as "ACCOUNT_RK",
            sum(p."CREDIT_AMOUNT") as "CREDIT_AMOUNT",
            coalesce(SUM(p."CREDIT_AMOUNT") * max(m."REDUCED_COURCE"), sum(p."CREDIT_AMOUNT")) as "CREDIT_AMOUNT_RUB"
        from "DS".ft_posting_f p
        left join "DS".ft_balance_f b
            on p."CREDIT_ACCOUNT_RK" = b."ACCOUNT_RK"
        left join "DS".md_exchange_rate_d m
            on b."CURRENCY_RK" = m."CURRENCY_RK"
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
        left join "DS".ft_balance_f b
            on p."DEBET_ACCOUNT_RK" = b."ACCOUNT_RK"
        left join "DS".md_exchange_rate_d m
            on b."CURRENCY_RK" = m."CURRENCY_RK"
            and p."OPER_DATE" between m."DATA_ACTUAL_DATE" and m."DATA_ACTUAL_END_DATE"
        where p."OPER_DATE" = i_OnDate
        group by p."DEBET_ACCOUNT_RK"
        order by "DEBET_ACCOUNT_RK" asc
    )

    insert into "DM".dm_account_turnover_f("ON_DATE", "ACCOUNT_RK", "CREDIT_AMOUNT", "CREDIT_AMOUNT_RUB", "DEBET_AMOUNT", "DEBET_AMOUNT_RUB")
    select
        i_OnDate as "ON_DATE",
        coalesce(c."ACCOUNT_RK", d."ACCOUNT_RK"),
        c."CREDIT_AMOUNT",
        c."CREDIT_AMOUNT_RUB",
        d."DEBET_AMOUNT",
        d."DEBET_AMOUNT_RUB"
    from credit_data c
    full join debet_data d on c."ACCOUNT_RK" = d."ACCOUNT_RK";
end;
$$ language plpgsql;

do $$
declare
    start_date date := '2018-01-31';
begin
    while start_date <= '2018-01-31' loop
        call "DS".fill_account_turnover_f(start_date);
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


create or replace procedure "DS".fill_account_balance_f(i_OnDate date)
as $$
begin
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
end;
$$ language plpgsql;

do $$
declare
    start_date date := '2018-01-01';
begin
    while start_date <= '2018-01-31' loop
        call "DS".fill_account_balance_f(start_date date);
        start_date := start_date + interval '1 day';
    end loop;
end;
$$ language plpgsql;
