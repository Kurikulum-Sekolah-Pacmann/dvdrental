{% snapshot dim_staff %}

{{
    config(
      target_database='dwh',
      target_schema='final',
      unique_key='sk_staff_id',

      strategy='check',
      check_cols=[
			'first_name',
			'last_name',
			'address',
			'email',
			'active',
			'username',
			'password',
			'district',
			'phone',
			'city',
			'country' 
		]
    )
}}

with stg__staff as (
	select *
	from {{ ref("stg_dwh__staff") }}
),

stg__address as (
	select *
	from {{ ref("stg_dwh__address") }}
),

stg__city as (
	select *
	from {{ ref("stg_dwh__city") }}
),

stg__country as (
	select *
	from {{ ref("stg_dwh__country") }}
),

dim_staff as (
	select 
		ss.staff_id as nk_staff_id,
		ss.first_name,
		ss.last_name,
		sa.address,
		ss.email,
		ss.active,
		ss.username,
		ss."password",
		sa.district,
		sa.phone,
		sc.city,
		sc2.country 
	from stg__staff ss 
	join stg__address sa 
		on sa.address_id = ss.address_id 
	join stg__city sc 
		on sc.city_id = sa.city_id 
	join stg__country sc2 
		on sc2.country_id = sc.country_id
),

final_dim_staff as (
	select
		{{ dbt_utils.generate_surrogate_key( ["nk_staff_id"] ) }} as sk_staff_id,  
		* 
	from dim_staff
)

select * from final_dim_staff

{% endsnapshot %}