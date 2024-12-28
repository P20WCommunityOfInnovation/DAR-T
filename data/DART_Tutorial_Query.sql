USE SI;
GO
/* This first CTE shows how to apply Top and Bottom Range. Notice that the actual counts are NULL, but the inequality is placed in the rate
   section. There are different ways to do this depending on the end reporting structure.
   
   When implementing Top/Bottom Range, this is the priority piece of code. Other important aspects are included in further CTEs*/
WITH TopBottom1 AS (
SELECT [OrganizationName]
      ,[OrganizationID]
      ,[GroupName]
	  ,GroupType
      ,[TotalStudents] AS TotalStudents_Unprotected -- The _Unprotected columns are used for comparison at the end step

	  ,CASE WHEN [NumberGraduated] < 3 OR TotalStudents - NumberGraduated < 3 
	        THEN NULL 
			ELSE [TotalStudents] 
			END AS TotalStudents

      ,NumberGraduated AS NumberGraduated_Unprotected

	  ,CASE WHEN [NumberGraduated] < 3 OR TotalStudents - NumberGraduated < 3 
	        THEN NULL 
			ELSE NumberGraduated 
			END AS NumberGraduated

	  ,GraduationRate AS GraduationRate_Unprotected

      ,CASE WHEN [NumberGraduated]*1.0 < 3 
	        THEN CONCAT('<', CAST(CAST(3.0/totalStudents*1.0 as decimal(5,5)) * 100 as varchar(8)), '%') -- The two CAST calls are to make the decimals not go on indefinitely and to make the character formatting play nice with the whole column
	        WHEN (TotalStudents - NumberGraduated) < 3 
			THEN CONCAT('>', CAST(CAST((1 - (3.0/totalStudents)) as decimal(5,5)) * 100 as varchar(8)), '%') -- All three of the outputs for the CASE statement need to be the same (e.g., varchar(8)). 
			ELSE CAST(CAST([GraduationRate]as decimal(5,5)) as varchar(8))
			END AS GraduationRate

      ,CASE WHEN [NumberGraduated] < 3 OR TotalStudents - NumberGraduated < 3 
	        THEN 'Top/Bottom' 
			END AS DAT_Reason -- Use the same logic as within the current step to apply DAT reason.
      
  FROM [dbo].[DAT_Tutorial]
  ),

/* The second CTE shows how to apply the fuzzy denominator. This Disclosure Avoidance Technique only works if the public does not know which 
number is used to generate the top/bottom range. However, OSPI's DAT document showed the information publically on the website for years and 
is still available on archives on the web (I can send links if desired). As such, we implemented a fuzzy denominator. The fuzzy denominator 
is only used for determining the '<' or '>' numbers and does not affect the actual graduation rates. I also have a mathematical proof that
I can provide to show that the ranges shown are still accurate to the original percentages.

This is second on the list of our priorities. */
TopBottom2 AS (
SELECT OrganizationName
      ,OrganizationID
	  ,GroupName
      ,TotalStudents_Unprotected
	  ,TotalStudents_fuzzy
	  ,CASE WHEN [NumberGraduated] < 3 OR TotalStudents - NumberGraduated < 3 
	        THEN NULL 
			ELSE [TotalStudents] 
			END AS TotalStudents	
      ,NumberGraduated_Unprotected
	  ,CASE WHEN [NumberGraduated] < 3 OR TotalStudents - NumberGraduated < 3 
	        THEN NULL 
			ELSE NumberGraduated 
			END AS NumberGraduated
      ,GraduationRate_Unprotected
	  ,CASE WHEN [NumberGraduated]*1.0 < 3 
	        THEN CONCAT('<', CAST(CAST(3.0/totalStudents*1.0 as decimal(5,5)) * 100 as varchar(8)), '%') -- The two CAST calls are to make the decimals not go on indefinitely and to make the character formatting play nice with the whole column
	        WHEN (TotalStudents - NumberGraduated) < 3 
			THEN CONCAT('>', CAST(CAST((1 - (3.0/totalStudents)) as decimal(5,5)) * 100 as varchar(8)), '%') -- All three of the outputs for the CASE statement need to be the same (e.g., varchar(8)). 
			ELSE CAST(CAST([GraduationRate]as decimal(5,5)) as varchar(8))
			END AS GraduationRate
	  ,CASE WHEN [NumberGraduated] < 3 OR TotalStudents - NumberGraduated < 3 
	               THEN 'Top/Bottom' 
			       END AS DAT_Reason 
FROM (SELECT OrganizationName
             ,OrganizationID
             ,GroupName
             ,[TotalStudents] AS TotalStudents_Unprotected
			 ,[TotalStudents]
             ,TotalStudents_fuzzy = CASE WHEN RAND(CHECKSUM(newid())) < 0.5 THEN TotalStudents+1 ELSE TotalStudents END
             ,NumberGraduated AS NumberGraduated_Unprotected
             ,NumberGraduated
             ,GraduationRate AS GraduationRate_Unprotected
             ,GraduationRate
        FROM [dbo].[DAT_Tutorial]
) AS fuzzy
),

/* It was determined also that for edge cases, it would be potentially feasible to undo protections if too many decimals of precision were 
shown. As such, we use the following code to determine how many decimals of precision to show in the final output. 

This is third on the list of our priorities. This CTE is the same as the second one, but with the decimal case statements added.*/

TopBottom3 AS (
SELECT OrganizationName
      ,OrganizationID
	  ,GroupName
      ,TotalStudents_Unprotected
	  ,TotalStudents_fuzzy
	  ,CASE WHEN [NumberGraduated] < 3 OR TotalStudents - NumberGraduated < 3 
	        THEN NULL 
			ELSE [TotalStudents] 
			END AS TotalStudents	
      ,NumberGraduated_Unprotected
	  ,CASE WHEN [NumberGraduated] < 3 OR TotalStudents - NumberGraduated < 3 
	        THEN NULL 
			ELSE NumberGraduated 
			END AS NumberGraduated
      ,GraduationRate_Unprotected
	  ,CASE WHEN [NumberGraduated]*1.0 < 3 
	        THEN Case when TotalStudents_fuzzy between 10 and 100 Then '<' + trim(cast(cast((3/TotalStudents_fuzzy)*100 as decimal(9,1)) as char)) + '%' 
                                                             when TotalStudents between 101 and 1000 Then '<' + trim(cast(cast((3/TotalStudents_fuzzy)*100 as decimal(9,2)) as char)) + '%' 
                                                             when TotalStudents between 1001 and 10000 Then '<' + trim(cast(cast((3/TotalStudents_fuzzy)*100 as decimal(9,3)) as char)) + '%' 
                                                             when TotalStudents between 10001 and 100000 Then '<' + trim(cast(cast((3/TotalStudents_fuzzy)*100 as decimal(9,4)) as char)) + '%' 
                                                             when TotalStudents between 100001 and 1000000 Then '<' + trim(cast(cast((3/TotalStudents_fuzzy)*100 as decimal(9,5)) as char)) + '%' 
 															END
	        
			WHEN (TotalStudents - NumberGraduated) < 3 
			THEN   Case when TotalStudents_fuzzy between 10 and 100 Then '>' + trim(cast(cast((1-(3/TotalStudents_fuzzy))*100 as decimal(9,1)) as char)) + '%' 
                                                             when TotalStudents between 101 and 1000 Then '>' + trim(cast(cast((1-(3/TotalStudents_fuzzy))*100 as decimal(9,2)) as char)) + '%' 
                                                             when TotalStudents between 1001 and 10000 Then '>' + trim(cast(cast((1-(3/TotalStudents_fuzzy))*100 as decimal(9,3)) as char)) + '%' 
                                                             when TotalStudents between 10001 and 100000 Then '>' + trim(cast(cast((1-(3/TotalStudents_fuzzy))*100 as decimal(9,4)) as char)) + '%' 
                                                             when TotalStudents between 100001 and 1000000 Then '>' + trim(cast(cast((1-(3/TotalStudents_fuzzy))*100 as decimal(9,5)) as char)) + '%' 
 															END
			ELSE CAST(CAST([GraduationRate]as decimal(5,4)) as varchar(8)) -- This is restricted to four so that it would have two decimals showing when put into a percentage format.
			END AS GraduationRate
	  ,CASE WHEN [NumberGraduated] < 3 OR TotalStudents - NumberGraduated < 3 
	               THEN 'Top/Bottom' 
			       END AS DAT_Reason 
FROM (SELECT OrganizationName
             ,OrganizationID
             ,GroupName
             ,[TotalStudents] AS TotalStudents_Unprotected
			 ,TotalStudents
             ,TotalStudents_fuzzy = CASE WHEN RAND(CHECKSUM(newid())) < 0.5 THEN TotalStudents+1 ELSE TotalStudents END
             ,NumberGraduated AS NumberGraduated_Unprotected
             ,NumberGraduated
             ,GraduationRate AS GraduationRate_Unprotected
             ,GraduationRate
        FROM [dbo].[DAT_Tutorial]
) AS fuzzy
)
--- CTEs Above ---
/* The query pulls the final data. You can comment and uncomment the "_Unprotected" fields to see the data before and after their transformation. */
SELECT OrganizationName
      ,OrganizationID
	  ,GroupName
      --,TotalStudents_Unprotected
	  --,TotalStudents_fuzzy
	  ,TotalStudents	
      --,NumberGraduated_Unprotected
	  ,NumberGraduated
      --,GraduationRate_Unprotected
	  ,GraduationRate
	  ,DAT_Reason
FROM TopBottom3