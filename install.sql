CREATE OR REPLACE EXTERNAL TABLE `analytics.int_alerting_config_kmeans`
OPTIONS(
  sheet_range="sources_kmeans",
  format="GOOGLE_SHEETS",
  uris=["https://docs.google.com/spreadsheets/d/..........."]
);
create OR REPLACE table analytics.alerting_train_log
(
    name         STRING,
    last_trained TIMESTAMP
);


create or replace view analytics.alerting_config_kmeans
as
(
select *,
       FORMAT(
               """
               analytics.alerting_%s OPTIONS (model_type='kmeans', num_clusters=4,standardize_features = TRUE
               --{WARM_START}--
               ) AS SELECT %s FROM %s
               where cast(%s as timestamp) > IFNULL((
                             select cast (timestamp_sub(max(%s), interval %d day) as timestamp)
                             from %s), timestamp_sub(CURRENT_TIMESTAMP(), interval %d day))
                   """,
               regexp_replace(NORMALIZE_AND_CASEFOLD(alert), '[^[:alnum:]]', '_'), ml_columns, table, date_column,
               date_column, train_window_days, table, train_window_days)  as model_ddl,
       FORMAT("""
       CREATE OR REPLACE VIEW analytics.alerting_alerts_%t OPTIONS(description=%T) as (
       SELECT * except (%t, %t),
       format(%t) as description,
       %t as entity,
       %t as date_column
FROM
    ML.DETECT_ANOMALIES(MODEL `analytics.alerting_%t`,
                        STRUCT (%F AS contamination),
                        (
                            SELECT *
                            FROM `%t`))
)


           """, regexp_replace(NORMALIZE_AND_CASEFOLD(alert), '[^[:alnum:]]', '_'), alert, date_column, entity_column,
              format_spec, entity_column, date_column,
              regexp_replace(NORMALIZE_AND_CASEFOLD(alert), '[^[:alnum:]]', '_'), anomaly_percentage,
              table)                                                      as view_ddl,
       regexp_replace(NORMALIZE_AND_CASEFOLD(alert), '[^[:alnum:]]', '_') as alert_name
FROM analytics.int_alerting_config_kmeans
where alert is not null);


create or replace procedure analytics.alerting_create_models(force_all BOOL)
BEGIN
    DECLARE
        models ARRAY <struct <name string, ddl string>>;
    DECLARE
        c int64;
    DECLARE q string;
    SET
        models = (
            SELECT ARRAY_AGG(struct (alert_name, model_ddl))
            FROM `analytics.alerting_config_kmeans`);
    SET
        c = 0;
    WHILE
        c < ARRAY_LENGTH(models)
        DO
            set q = (select models[
                                OFFSET
                                    (c)].ddl);
            if force_all = TRUE then
                set q = CONCAT("CREATE OR REPLACE MODEL ", q);
            else
                set q = CONCAT("CREATE MODEL ", q);
            end if;
            begin
                EXECUTE IMMEDIATE q;
                insert into analytics.alerting_train_log (name, last_trained)
                VALUES (models[
                            OFFSET
                                (c)].name, CURRENT_TIMESTAMP());
            exception
                when error then
                select format("ERROR CREATING %t", @@error.message);
            END;
            SET
                c = c + 1;
        END WHILE;
END;


create or replace procedure analytics.alerting_create_models(force_all BOOL)
BEGIN
    DECLARE
        models ARRAY <struct <name string, ddl string>>;
    DECLARE
        c int64;
    DECLARE q string;
    SET
        models = (
            SELECT ARRAY_AGG(struct (alert_name, model_ddl))
            FROM `analytics.alerting_config_kmeans`);
    SET
        c = 0;
    WHILE
        c < ARRAY_LENGTH(models)
        DO
            set q = (select models[
                                OFFSET
                                    (c)].ddl);
            if force_all = TRUE then
                set q = CONCAT("CREATE OR REPLACE MODEL ", q);
            else
                set q = CONCAT("CREATE MODEL ", q);
            end if;
            begin
                EXECUTE IMMEDIATE q;
                insert into analytics.alerting_train_log (name, last_trained)
                VALUES (models[
                            OFFSET
                                (c)].name, CURRENT_TIMESTAMP());
            exception
                when error then
                select format("ERROR CREATING %t", @@error.message);
            END;
            SET
                c = c + 1;
        END WHILE;
END;

create or replace procedure analytics.alerting_retrain_models(force_all BOOL)
BEGIN
    DECLARE
        models ARRAY <struct <name string, ddl string>>;
    DECLARE
        c int64;
    DECLARE q string;
    if force_all = FALSE then
        SET
            models = (
                select ARRAY_AGG(struct (alert_name, model_ddl))
                from (
                         select retrain_interval_days,
                                alert_name,
                                last_trained,
                                DATETIME_ADD(last_trained, interval retrain_interval_days day) as next_train,
                                model_ddl
                         from analytics.alerting_config_kmeans
                                  left join (select max(last_trained) as last_trained, name
                                             from analytics.alerting_train_log
                                             group by name)
                                            on alert_name = name)
                where next_train < CURRENT_TIMESTAMP());
    else
        SET
            models = (
                select ARRAY_AGG(struct (alert_name, model_ddl)) from analytics.alerting_config_kmeans);
    end if;
    SET
        c = 0;
    WHILE
        c < ARRAY_LENGTH(models)
        DO
            set q = (select models[
                                OFFSET
                                    (c)].ddl);
            set q = CONCAT("CREATE OR REPLACE MODEL ", q);
            set q = REGEXP_REPLACE(q, '--{WARM_START}--', ', WARM_START=true');
            begin
                EXECUTE IMMEDIATE q;
                --select q;
                insert into analytics.alerting_train_log (name, last_trained)
                VALUES (models[
                            OFFSET
                                (c)].name, CURRENT_TIMESTAMP());
            exception
                when error then
                select format("ERROR TRAINING %t", @@error.message);
            END;
            SET
                c = c + 1;
        END WHILE;
END;

create or replace procedure analytics.alerting_create_kmeans_views()
BEGIN
    DECLARE
        views ARRAY <string>;
    DECLARE
        c int64;
    SET
        views = (
            SELECT ARRAY_AGG(view_ddl)
            FROM `analytics.alerting_config_kmeans`);
    SET
        c = 0;
    WHILE
        c < ARRAY_LENGTH(views)
        DO
            EXECUTE IMMEDIATE
                views[
                    OFFSET
                        (c)];
            SET
                c = c + 1;
        END WHILE;
END;

create or replace procedure analytics.alerting_create_aggregated_view()
begin
    declare uq string;
    declare uqs array <string>;
    set uqs = (select array_agg(format(
            """select %T as alert_id,%T as alert, date_column,entity, description, is_anomaly from analytics.%t""",
            table_name,
            description,
            table_name))
               from (select JSON_EXTRACT_SCALAR(option_value) as description, table_name
                     from analytics.INFORMATION_SCHEMA.TABLE_OPTIONS
                     where table_name like 'alerting_alerts_%'
                       and option_name = 'description'));
    set uq = (select string_agg (a, "\n UNION ALL\n") from unnest(uqs) as a);
    execute immediate CONCAT("CREATE OR REPLACE VIEW analytics.alerting_all as (",uq,")");
end;

create or replace procedure analytics.alerting_bootstrap()
begin 
call analytics.alerting_create_models(TRUE);
call analytics.alerting_create_kmeans_views();
call analytics.alerting_create_aggregated_view();
end;
