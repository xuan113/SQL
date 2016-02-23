select cell,
count(*)
from ab_events
where test = '6726'
and dateint = 20151115
and hour = 20
and eventtype = 'Allocation'
group by 1
order by 1
;