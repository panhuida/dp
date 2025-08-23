insert into public.ads_orders
select
    *
from
    public.fct_orders
limit 1
;