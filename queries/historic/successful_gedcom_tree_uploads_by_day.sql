DECLARE @AnalysisStart DATETIME;
DECLARE @AnalysisEnd DATETIME;
SET @AnalysisStart = CONVERT(DATE, DATEADD(DAY, -367, GETDATE()));
SET @AnalysisEnd = CONVERT(DATE, DATEADD(DAY, -1, GETDATE()));

SELECT 'tree.uploads.per_day.count', NumTrees, DATEDIFF(SECOND,{d '1970-01-01'}, TreeUploadDate) FROM
(
	SELECT CONVERT(DATE, tree.DateCreated) AS TreeUploadDate, COUNT(1) AS NumTrees
	FROM [Data_Tree].[dbo].[FamilyTreeTransform] tree_transform
	  INNER JOIN [Data_Tree].[dbo].[FamilyTree] tree ON tree_transform.FamilyTreeId = tree.Id
	  INNER JOIN data_tree..RelationServiceMember RSM ON tree.RelationServiceMemberId = rsm.Id
	  INNER JOIN [Data_FMP]..[all_member] member ON rsm.MemberKey = member.member_key
	WHERE CONVERT(DATE, tree_transform.DateCreated) BETWEEN @AnalysisStart AND @AnalysisEnd -- Tree was uploaded within the window
	  AND member.partnership_key = 0 -- US user
	  AND tree_transform.DataHeader IS NOT NULL -- Upload was successful
	GROUP BY CONVERT(DATE, tree.DateCreated)
) AS TreeUploadList
ORDER BY TreeUploadDate ASC