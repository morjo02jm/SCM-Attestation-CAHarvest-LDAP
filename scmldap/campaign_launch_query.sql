select
     concat('Product: '+ substring(github.[EntitlementAttributes],9,len(github.[EntitlementAttributes])-8)),
            '/ Broker: ' + github.[EntitlementOwner1] ) as 'ResName1',
     'Project: '+github.[EntitlementOwner2] as 'ResName2',
     case
        when 
     case 
        when len(github.[ContactEmail]) < 1 then 'Unknown' 
        else left(github.[ContactEmail], charindex('@',github.[ContactEmail])-1)  
     end as 'Contact',     
     github.[User_ID] as 'PersonID',
     case
       when right(github.[User_ID],1) = '?' then  'Internal: ' + left(github.[User_ID],charindex('?',github.[User_ID])-1) + '- ('+ left(github.[User_ID],charindex('?',github.[User_ID])-1) +')'
       when (emp.FirstName + ' '+ emp.LastName + '- (' +github.[User_ID] + ')') is null then
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
        select distinct R1.Application, R1.ApplicationLocation, R1.EntitlementAttributes, R1.EntitlementOwner1, R1.ContactEmail, R1.User_ID 
        from GITHUB_REVIEW R1
     ) github
     where github.application = 'CA Harvest SCM'
       and github.ApplicationLocation like 'L1AGUSDB00%'