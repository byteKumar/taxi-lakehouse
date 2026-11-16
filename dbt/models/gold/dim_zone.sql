{{ config(materialized='table') }}
-- TLC taxi zones. 264/265 are TLC's own "Unknown" / "Outside of NYC" codes.

select
    LocationID                                  as zone_id,
    coalesce(Borough, 'Unknown')                as borough,
    coalesce(Zone, 'Unknown')                   as zone,
    coalesce(service_zone, 'Unknown')           as service_zone,
    service_zone in ('Airports', 'EWR')         as is_airport,
    case LocationID when 1 then 'EWR' when 132 then 'JFK' when 138 then 'LGA' end
                                                as airport_code
from {{ ref('taxi_zone_lookup') }}
