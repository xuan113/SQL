-- slow: run presto under terminal
-- copy short links from go/wings and paste here
select count(*), other_properties['OUTPUT'] from 
default.cmp_panoramix where 
dateint >= 20151004 and 
dateint < 20151128 and 
other_properties['COMPONENT'] = 'SIMPLEURL' and 
other_properties['capp'] = 'simpleurl' and 
(other_properties['OUTPUT'] = 'elX7fS9ji6m' or
other_properties['OUTPUT'] = 'kpAdedmfMJ2' or
other_properties['OUTPUT'] = 'fDm6szd19i0' or
other_properties['OUTPUT'] = 'ShGxELUwy4' or
other_properties['OUTPUT'] = 'hUgeqDjuPu0' or
other_properties['OUTPUT'] = 'eaXfjzv9nvU' or
other_properties['OUTPUT'] = 'Fc2rLO2VSS') group by
other_properties['OUTPUT']