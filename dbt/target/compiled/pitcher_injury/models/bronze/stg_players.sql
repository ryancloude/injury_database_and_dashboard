

with src as (
    select * from "baseball"."bronze"."players"
)

select
    id as person_id,
    fullname,
    firstname,
    lastname,
    firstname || ' ' || lastname as secondary_name,
    primarynumber,
    birthdate,
    birthcity,
    birthstateprovince,
    birthcountry,
    height,
    weight,
    usename,
    uselastname,
    middlename,
    draftyear,
    strikezonetop,
    strikezonebottom,
    primaryposition_code,
    batside_code,
    pitchhand_code,
    nickname

from src