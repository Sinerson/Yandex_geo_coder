all_addr = """
select A.ADDRESS_CODE,
       rtrim(C.COUNTRY_NAME) as COUNTRY_NAME,
       rtrim(COUNTRY_REGION_NAME) as COUNTRY_REGION_NAME,
       rtrim(AREA_REGION_NAME) as AREA_REGION_NAME,
       T.TOWN_PREFIX||rtrim(T.TOWN_NAME) as TOWN_NAME,
       S.STREET_PREFIX||' '||rtrim(S.STREET_NAME) as STREET_NAME,
       convert(varchar(10),A.HOUSE)||A.HOUSE_POSTFIX as HOUSE
from INTEGRAL..ADDRESS A
join INTEGRAL..STREETS S on A.STREET_CODE = S.STREET_CODE
join INTEGRAL..TOWNS T on S.TOWN_CODE = T.TOWN_CODE
join INTEGRAL..AREA_REGIONS AR on T.AREA_REGION_CODE = AR.AREA_REGION_CODE
join INTEGRAL..COUNTRIES C on T.COUNTRY_CODE = C.COUNTRY_CODE
join INTEGRAL..COUNTRY_REGIONS CR on T.COUNTRY_REGION_CODE = CR.COUNTRY_REGION_CODE
where rtrim(C.COUNTRY_NAME) = 'Россия' and
      rtrim(CR.COUNTRY_REGION_NAME) like '%' + 'Нижегородская' + '%' and
      rtrim(AR.AREA_REGION_NAME) like '%' + 'Кстовский' + '%' and
      S.STREET_NAME not like ('%не использовать%')
group by A.ADDRESS_CODE, rtrim(C.COUNTRY_NAME), rtrim(COUNTRY_REGION_NAME), rtrim(AREA_REGION_NAME),
         T.TOWN_PREFIX||rtrim(T.TOWN_NAME), S.STREET_PREFIX||' '||rtrim(S.STREET_NAME), convert(varchar(10),A.HOUSE)||A.HOUSE_POSTFIX
 order by 1
"""