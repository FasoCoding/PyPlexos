CREATE SCHEMA IF NOT EXISTS silver;

CREATE VIEW silver.link_generator_node AS (
    SELECT 
        gen_obj.object_id as id_generator,
        node_obj.object_id as id_node,
        gen_obj.name AS generator,
        node_obj.name AS node,
    FROM ((bronze.t_membership
    INNER JOIN bronze.t_object as node_obj ON t_membership.child_object_id = node_obj.object_id)
    INNER JOIN bronze.t_object as gen_obj ON t_membership.parent_object_id = gen_obj.object_id)
    WHERE node_obj.class_id = 22 
    AND gen_obj.class_id = 2 
    AND t_membership.collection_id = 12
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


