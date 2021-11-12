create or replace procedure analytics.stat_sig(query string, noise_floor int64, num_tests int64)
BEGIN
    DECLARE q string;
    set q = """with data as (select metric
                  from (
                           select cast(cols as struct <f0 float64>).f0 as metric
                           from (
                                  %s
                                ) as cols)
                  where metric is not null
                  order by rand())
    select distinct round_div, round(avg(in_row) over ( partition by round_div)) as average_row
    from (
             select cast(round(avg_div * 100) as int64) as round_div, *
             from (select *
                   from (
                            select *,
                                   ABS(1 -
                                       SAFE_DIVIDE(rolling_avg,
                                                   lag(rolling_avg) over (partition by num order by in_row asc))) as avg_div
                            from (
                                     select *,
                                            avg(metric)
                                                over (partition by num order by rn ROWS BETWEEN UNBOUNDED PRECEDING and CURRENT ROW ) as rolling_avg,
                                            ROW_NUMBER() over (partition by num order by rn)                                          as in_row
                                     from (
                                              select *, mod(rn, %d) as num
                                              from (select metric, row_number() over () as rn
                                                    from data
                                                   ))
                                 ))
                   where avg_div is not null
                   order by avg_div desc))
    where round_div <= %d
    order by round_div desc
    limit 1""";
    EXECUTE IMMEDIATE FORMAT(q,query, num_tests, noise_floor);
END
