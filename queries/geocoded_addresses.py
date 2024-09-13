existing_addresses = """
select ADDRESS_CODE
from SV..ADDRESSES_COORD
order by ADDRESS_CODE
"""

address_without_post_code ="""
select A.ADDRESS_CODE,
       isnull(rtrim(C.COUNTRY_NAME),null) COUNTRY, isnull(rtrim(CR.COUNTRY_REGION_NAME),null) REGION_NAME,
       isnull(rtrim(AR.AREA_REGION_NAME),null) AREA_NAME,
       isnull(rtrim(T.TOWN_PREFIX)||rtrim(T.TOWN_NAME), null) TOWN,
       rtrim(S.STREET_NAME) STREET,
       case when A.HOUSE_POSTFIX is not null then cast(A.HOUSE as varchar(5))||' корп.'||A.HOUSE_POSTFIX else
           cast(A.HOUSE as varchar(5)) end as HOUSE
from INTEGRAL..ADDRESS A
join INTEGRAL..STREETS S on A.STREET_CODE = S.STREET_CODE
join INTEGRAL..TOWNS T on S.TOWN_CODE = T.TOWN_CODE
left join INTEGRAL..AREA_REGIONS AR on T.AREA_REGION_CODE = AR.AREA_REGION_CODE
left join INTEGRAL..COUNTRY_REGIONS CR on AR.COUNTRY_REGION_CODE = CR.COUNTRY_REGION_CODE
left join INTEGRAL..COUNTRIES C on T.COUNTRY_CODE = C.COUNTRY_CODE and CR.COUNTRY_CODE = C.COUNTRY_CODE
where POST_INDEX is null
"""

update_post_code = """
begin
begin tran
update INTEGRAL..ADDRESS
set POST_INDEX = ?
where ADDRESS_CODE = ?
commit tran
select @@transtate
end
"""
