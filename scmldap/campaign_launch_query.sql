select
     concat('Broker: '+ github.[EntitlementOwner1],
            '/ Project: ' + github.[EntitlementOwner2] , 
            '/ State: ' + github.[EntitlementName]) as 'ResName1',
     '-' as 'ResName2',
     left(github.[ContactEmail], charindex('@',github.[ContactEmail])-1) as 'ResName3',
     github.[User_ID] as 'PersonID',
     case
       when right(github.[User_ID],1) = '?' then  'Internal: ' + left(github.[User_ID],charindex('?',github.[User_ID])-1) + '- ('+ left(github.[User_ID],charindex('?',github.[User_ID])-1) +')'
       when (emp.FirstName + ' '+ emp.LastName + '- (' +github.[User_ID] + ')') is null then
           case when github.[User_ID] not like '[a-z$][a-z$][a-z$][a-z$][a-z$][0-9][0-9]' then 'Generic: ' + github.[User_ID] + '- ('+ github.[User_ID] +')'
            else
                github.[User_ID]  + '- ('+ github.[User_ID] +')'
            end       
       else (emp.FirstName + ' '+ emp.LastName + '- (' +github.[User_ID] + ')') 
     end as 'UserName',
     case when emp.Company is null then 'CA' else emp.Company end as 'Organization',
     case when emp.Department is null then 'CA' else emp.Department end as 'Organization Type',
     emp.Email as 'Email', left(github.[ContactEmail],charindex('@',github.[ContactEmail])-1) as 'Manager ID',
     'N/A' as 'Manager Name',
     emp.Title as 'Title',
     emp.FirstName as 'First Name',
     emp.LastName as 'Last Name',
     'Active ('+emp.Status+')' as 'Status'
     FROM /* GITHUB_REVIEW as github */
     (     
        select distinct R1.Application, R1.ApplicationLocation, R1.EntitlementOwner1, R1.EntitlementOwner2, R1.EntitlementName, R1.ContactEmail, R1.User_ID 
        from GITHUB_REVIEW R1
     ) github
     left join QAR_EMPINFO as emp on emp.SAM=github.[User_ID]
     left join QAR_EMPINFO as empmgr1 on empmgr1.SAM = ltrim(rtrim(replace(substring(emp.ManagerDN,0,charindex(',',emp.ManagerDN)),'CN=',''))) 
     where github.application = 'CA Harvest SCM'
       and github.ApplicationLocation like 'L1AGUSDB00%'