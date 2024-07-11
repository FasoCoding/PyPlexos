create table temp_dispatch_table as (
    select 
        t_key.membership_id as id_generator,
        t_data_0.period_id as id_period,
        t_property.name as property,
        t_data_0.value as gen_value
    from bronze.t_data_0
    inner join bronze.t_key on t_key.key_id = t_data_0.key_id
    inner join bronze.t_property on t_property.property_id = t_key.property_id
    where t_property.collection_id = 1
);