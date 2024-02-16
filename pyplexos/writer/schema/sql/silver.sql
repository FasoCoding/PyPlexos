CREATE SCHEMA IF NOT EXISTS silver;

CREATE VIEW silver.dim_centrales AS (
    select  
        t_membership.membership_id as id_central,
        t_object.name as central
    from bronze.t_membership
    inner join bronze.t_object ON t_object.object_id = t_membership.child_object_id
    where t_membership.collection_id == 1
    order by t_membership.membership_id
);

CREATE VIEW silver.dim_fechas AS (
    select 
        t_phase_3.interval_id as id_intervalo,
        t_phase_3.period_id as id_periodo,
        t_period_0.datetime as fecha_hora,
        t_period_0.datetime::date as fecha,
        extract('hour' from t_period_0.datetime::time) as hora
    from bronze.t_phase_3
    inner join bronze.t_period_0 on t_period_0.interval_id = t_phase_3.interval_id
    order by t_phase_3.interval_id
);

CREATE VIEW silver.fct_generacion AS (
    select 
        t_key.membership_id as id_central,
        t_data_0.period_id as id_periodo,
        t_data_0.value as generacion
    from bronze.t_data_0
    inner join bronze.t_key on t_key.key_id = t_data_0.key_id
    inner join bronze.t_property on t_property.property_id = t_key.property_id
    where t_property.collection_id = 1 and t_property.name = 'Generation'
    order by t_data_0.value, t_key.membership_id
);
