addr_ins = """
insert into SV..ADDRESSES_COORD(ADDRESS_CODE, LONGITUDE, LATITUDE) values (?, ?, ?)
"""

update_address_to_unload = """
begin
select ADDRESS_CODE as ADDR_CODE,
       POST_INDEX as PST_INDEX
into #TempAddr
from INTEGRAL..ADDRESS
where POST_INDEX is not null
begin tran
update INTEGRAL..UNLOAD_ABN_ADDR
set POST_INDEX = A.PST_INDEX
from #TempAddr A
where ADDRESS_CODE = A.ADDR_CODE and
      POST_INDEX is null
commit tran
drop table #TempAddr
select @@transtate
end
"""