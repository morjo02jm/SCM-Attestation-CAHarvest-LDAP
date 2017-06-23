select distinct
     'Product:'+
     case
      when len(R1.EntitlementAttributes)<9 then 'Unknown'
      else substring(R1.EntitlementAttributes,9,len(R1.EntitlementAttributes)-8) 
     end  +
     '/Broker:'+R1.EntitlementOwner1+
     '/Project:'+R1.EntitlementOwner2 as 'Entitlement', 
     case 
        when len(R1.ContactEmail) < 1 then 'Unknown' 
        else left(R1.ContactEmail, charindex('@',R1.ContactEmail)-1)+'@ca.com'  
     end as 'Contact',
     case 
        when right(R1.User_ID,1) = '?' then  'Internal: ' + left(R1.User_ID,charindex('?',R1.User_ID)-1) 
        case when R1.User_ID not like '[a-z$][a-z$][a-z$][a-z$][a-z$][0-9][0-9]' then 'Generic: ' + R1.User_ID 
        else 'User: '+ R1.User_ID 
     end as 'User ID'             
FROM GITHUB_REVIEW R1 
WHERE R1.application = 'CA Harvest SCM'
 AND  R1.ApplicationLocation like 'L1AGUSDB00%'
order by 1,2,3