{% snapshot fct_rental %}

{{
    config(
      target_database='dwh',
      target_schema='final',
      unique_key='sk_rental_id',

      strategy='check',
      check_cols=['payment_amount']
    )
}}

with stg_rental as (
    select *
    from {{ ref("stg_dwh__rental") }}
),

dim_customer as (
    select *
    from {{ ref("dim_customer") }}
),

dim_staff as (
    select *
    from {{ ref("dim_staff") }}
),

dim_store as (
    select *
    from {{ ref("dim_store") }}
),

dim_date as (
    select *
    from {{ ref("dim_date") }}
),

dim_film as (
    select *
    from {{ ref("dim_film") }}
),

stg_inventory as (
    select *
    from {{ ref("stg_dwh__inventory") }}
),

stg_payment as (
    select *
    from {{ ref("stg_dwh__payment") }}
),

fct_rental as (
    select 
        sr.rental_id as nk_rental_id,
        dc.sk_customer_id,
        df.sk_film_id,
        ds.sk_store_id,
        ds2.sk_staff_id,
        dd.date_id as rental_date_id,
        dd2.date_id as return_date_id,
        dd3.date_id as payment_date_id,
        sp.amount as payment_amount
    from stg_rental sr
    join dim_customer dc
        on dc.nk_customer_id = sr.customer_id 
    join dim_date dd
        on dd.date_actual = DATE(sr.rental_date)
    join dim_date dd2
        on dd2.date_actual = DATE(sr.return_date)
    join stg_inventory si
        on si.inventory_id = sr.inventory_id 
    join dim_film df
        on df.nk_film_id = si.film_id 
    join dim_store ds
        on ds.nk_store_id = si.store_id 
    join dim_staff ds2
        on ds2.nk_staff_id = sr.staff_id 
    join stg_payment sp
        on sp.rental_id = sr.rental_id 
    join dim_date dd3
        on dd3.date_actual = DATE(sp.payment_date)
),

final_fct_rental as (
    select
        {{ dbt_utils.generate_surrogate_key( ["nk_rental_id"] ) }} as sk_rental_id,  
        *
    from fct_rental
)

select * from final_fct_rental

{% endsnapshot %}