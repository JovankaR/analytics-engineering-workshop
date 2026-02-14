with source_data as (
    select
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        payment_type
    from {{ ref('int_trips_unioned') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by 
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                pickup_location_id,
                dropoff_location_id
            order by pickup_datetime
        ) as rn
    from source_data
),

cleaned as (
    select
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        total_amount,
        payment_type
    from deduplicated
    where rn = 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'c.vendor_id',
            'c.pickup_datetime',
            'c.dropoff_datetime',
            'c.pickup_location_id',
            'c.dropoff_location_id',
            'c.trip_distance',
            'c.total_amount'
        ]) }} as trip_id,

/*
        to_hex(md5(concat(
            coalesce(cast(c.vendor_id as string), ''),
            coalesce(cast(c.pickup_datetime as string), ''),
            coalesce(cast(c.dropoff_datetime as string), ''),
            coalesce(cast(c.pickup_location_id as string), ''),
            coalesce(cast(c.dropoff_location_id as string), ''),
            coalesce(cast(c.trip_distance as string), ''),
            coalesce(cast(c.total_amount as string), '')
        ))) as trip_id,
*/
        c.vendor_id,
        dv.vendor_name,

        c.pickup_datetime,
        c.dropoff_datetime,

        c.pickup_location_id,
        dz_pickup.zone as pickup_zone,
        dz_pickup.borough as pickup_borough,

        c.dropoff_location_id,
        dz_dropoff.zone as dropoff_zone,
        dz_dropoff.borough as dropoff_borough,

        c.passenger_count,
        c.trip_distance,
        c.fare_amount,
        c.tip_amount,
        c.total_amount,

        c.payment_type,

        {{ get_payment_type_desc('c.payment_type') }} as payment_type_desc

    from cleaned c
    left join {{ ref('dim_vendors') }} dv
        on c.vendor_id = dv.vendor_id
    left join {{ ref('dim_zones') }} dz_pickup
        on c.pickup_location_id = dz_pickup.location_id
    left join {{ ref('dim_zones') }} dz_dropoff
        on c.dropoff_location_id = dz_dropoff.location_id
)

select *
from final
