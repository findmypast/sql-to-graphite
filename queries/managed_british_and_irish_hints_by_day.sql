DECLARE @AnalysisStart DATETIME;
DECLARE @AnalysisEnd DATETIME;
SET @AnalysisStart = CONVERT(DATE, DATEADD(DAY, -367, GETDATE()));
SET @AnalysisEnd = CONVERT(DATE, DATEADD(DAY, -1, GETDATE()));

-- Number of managed hints per day
SELECT 'hints.managed.british_and_irish.per_day.count', NumManagedHints, DATEDIFF(SECOND,{d '1970-01-01'}, HintManagedOn) FROM 
(
	SELECT CONVERT(DATE, hint.DateUpdated) AS HintManagedOn, COUNT(1) AS NumManagedHints 
	FROM [Data_Tree]..[PersonHint] hint
	  INNER JOIN [Data_Tree]..[FamilyTree] tree ON tree.Id = hint.FamilyTreeId
	  INNER JOIN data_tree.dbo.RelationServiceMember rsm ON rsm.Id = tree.RelationServiceMemberId
	  INNER JOIN [Data_FMP]..[all_member] member ON rsm.MemberKey = member.member_key
	WHERE hint.HintStatusId != 0 -- Managed hint
	  AND CONVERT(DATE, hint.DateUpdated) BETWEEN @AnalysisStart AND @AnalysisEnd -- Within analysis window
	  AND hint.DateUpdated != hint.DateCreated -- Avoid accidentally pulling in new hints
	  AND member.partnership_key = 0 -- US user
	  AND (hint.SourceCountry = 'Great Britain' OR hint.SourceCountry = 'Ireland')
	GROUP BY CONVERT(DATE, hint.DateUpdated)
) AS ManagedHintsPerDay	
ORDER BY CONVERT(DATE, HintManagedOn) ASC
