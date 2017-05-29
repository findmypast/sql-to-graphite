IF OBJECT_ID('tempdb..#USIntermediateUsers') IS NOT NULL
  DROP TABLE #USIntermediateUsers
GO  

IF OBJECT_ID('tempdb..#USIntermediateUsersWithManagedHint') IS NOT NULL
  DROP TABLE #USIntermediateUsersWithManagedHint
GO

IF OBJECT_ID('tempdb..#ShowMeTheMoney') IS NOT NULL
  DROP TABLE #ShowMeTheMoney
GO  

DECLARE @PeriodStart DATETIME
DECLARE @PeriodEnd DATETIME
DECLARE @AnalysisDate DATETIME
DECLARE @AnalysisWindow int

SET @AnalysisDate = DATEADD(MONTH, -6, GETDATE())
SET @AnalysisWindow = 30

SET @PeriodStart = CONVERT(DATE, @AnalysisDate)
SET @PeriodEnd = DATEADD(MILLISECOND, -2, DATEADD(DAY, 1, @PeriodStart))

-- Funnel stage 1: US Int users who have registered and uploaded their first tree via GEDCOM upload during the period
SELECT * INTO #USIntermediateUsers FROM
(
	SELECT MemberKey, MemberRegisteredOn, TreeCreatedOn, RowNumber, TreeStatusId, TreeStatusNote, TreeId
	FROM (
		SELECT member_key AS MemberKey, member.date_created AS MemberRegisteredOn, TreeStatusId, TreeStatusNote, tree.DateCreated AS TreeCreatedOn, tree.Id AS TreeId,
			RANK() OVER (PARTITION BY RelationServiceMemberId ORDER BY tree.DateCreated ASC) AS RowNumber
		FROM [Data_FMP]..[all_member] AS member
		LEFT JOIN [Data_Tree]..[RelationServiceMember] AS rsm ON rsm.MemberKey = member.member_key
		INNER JOIN [Data_Tree]..[FamilyTree] AS tree ON tree.RelationServiceMemberId = rsm.Id
		WHERE 
			member.date_created BETWEEN @PeriodStart AND @PeriodEnd -- registered within the analysis period
			AND member.partnership_key = 10 -- Is a US member, NOTE: NOT USING RelationServiceMember.PartnershipKey as PartnershipKey = 0 refers to both US and UK members from 4/11/15 onwards
	) AS DateOrderedTrees
	INNER JOIN [Data_Tree].[dbo].[FamilyTreeTransform] tree_transform ON DateOrderedTrees.TreeId = tree_transform.FamilyTreeId -- Gedcom uploads
	WHERE DateOrderedTrees.RowNumber = 1 -- Takes earliest created tree
		AND tree_transform.DataHeader IS NOT NULL -- Upload was successful
		AND TreeCreatedOn < DATEADD(DAY, @AnalysisWindow, MemberRegisteredOn)
) AS funnel_1

-- Funnel stage 2: Users who have interacted with a hint in any way (i.e. a managed hint) [todo: before they have paid us money]
SELECT * INTO #USIntermediateUsersWithManagedHint FROM
(
	SELECT MemberKey, MemberRegisteredOn, ManagedFirstHintOn FROM (
		SELECT MemberKey, MemberRegisteredOn, DateUpdated AS ManagedFirstHintOn, RANK() OVER(PARTITION BY MemberKey ORDER BY DateUpdated ASC) AS HintNumber
		FROM (
			SELECT * FROM #USIntermediateUsers users
			LEFT JOIN [Data_Tree]..[PersonHint] AS hint ON hint.FamilyTreeId = users.TreeId
			WHERE hint.HintStatusId != 0 -- Hint is in any other state than 'New'
		) AS ranked_hints
	) AS members_with_hints
	WHERE HintNumber = 1
	  AND ManagedFirstHintOn < DATEADD(DAY, @AnalysisWindow, MemberRegisteredOn)
) AS funnel_2

-- Funnel stage 3: Have paid us (subs or PPV)
SELECT * INTO #ShowMeTheMoney FROM
(
	SELECT users_with_hints.MemberKey AS MemberKey, trans.currency_amount, trans.package_key 
	FROM #USIntermediateUsersWithManagedHint users_with_hints
	LEFT JOIN [Data_FMP]..[member_trans] AS trans ON trans.member_key = users_with_hints.MemberKey
	WHERE trans.trans_added_date < DATEADD(DAY, @AnalysisWindow, MemberRegisteredOn) -- Paid within 30 days of registering
	  AND trans.trans_added_date > ManagedFirstHintOn -- The user gave us money after interacting with a hint
	  AND trans.trans_status_key = 20 -- The transaction was successful
	  AND trans.currency_amount > 0 -- We got some money (wasn't a free trial)
	GROUP BY users_with_hints.MemberKey, trans.currency_amount, trans.package_key -- Only count multiple transactions per member once
) AS funnel_3

-- Stick into stats table
DECLARE @StatsTable TABLE (
	GraphitePath VARCHAR(256),
	MemberCount int,
	[Timestamp] int
);

DECLARE @GraphitePathRoot VARCHAR(256)
SET @GraphitePathRoot = 'test.funnels.hints.window.' + RTRIM(CONVERT(char, @AnalysisWindow)) + '_days.'

DECLARE @Timestamp int
SET @Timestamp = DATEDIFF(SECOND,{d '1970-01-01'}, @PeriodStart)

INSERT INTO @StatsTable SELECT @GraphitePathRoot + 'stage_1', (SELECT COUNT(*) FROM #USIntermediateUsers), @Timestamp
INSERT INTO @StatsTable SELECT @GraphitePathRoot + 'stage_2', (SELECT COUNT(*) FROM #USIntermediateUsersWithManagedHint), @Timestamp
INSERT INTO @StatsTable SELECT @GraphitePathRoot + 'stage_3', (SELECT COUNT(*) FROM #ShowMeTheMoney), @Timestamp

SELECT * FROM @StatsTable

DROP TABLE #USIntermediateUsers;
DROP TABLE #USIntermediateUsersWithManagedHint;
DROP TABLE #ShowMeTheMoney;