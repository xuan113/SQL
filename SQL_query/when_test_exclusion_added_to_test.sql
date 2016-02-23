select dateint, hour, user, message, changes from abadmin_log 
where test_id=XXXX and dateint>=20150601 
and action='updateTestExclusions'
order by dateint