/****** Object:  Table [dbo].[UsageLog]    Script Date: 11/22/2019 11:12:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[UsageLog](
	[Id] [uniqueidentifier] NOT NULL,
	[Timestamp] [datetime] NULL,
	[Level] [nvarchar](50) NULL,
	[MessageTemplate] [nvarchar](200) NULL,
	[Properties] [nvarchar](max) NULL,
	[Renderings] [nvarchar](max) NULL,
	[ActionId] [nvarchar](200) NULL,
	[ActionName] [nvarchar](200) NULL,
	[ConnectionId] [nvarchar](200) NULL,
	[RequestId] [nvarchar](200) NULL,
	[RequestPath] [nvarchar](200) NULL,
	[SessionId] [nvarchar](200) NULL,
	[SourceContext] [nvarchar](200) NULL,
	[User] [nvarchar](200) NULL,
 CONSTRAINT [PK_UsageLog] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  View [dbo].[v_DatasetQuery]    Script Date: 11/22/2019 11:12:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






CREATE VIEW [dbo].[v_DatasetQuery] AS

WITH X AS
(
	SELECT
		L.[Timestamp]
	  , L.[MessageTemplate]
	  , L.[ActionId]
	  , L.[ActionName]
	  , L.[RequestId]
	  , L.[RequestPath]
	  , L.[SessionId]
	  , L.[SourceContext]
	  , L.[User]
	FROM [dbo].[UsageLog] AS L
	WHERE L.MessageTemplate = 'Dataset starting. QueryRef:{QueryRef} DatasetRef:{DatasetRef}'
)
, Q AS
(
	SELECT
	    L.[Timestamp]
	  , L.[RequestId]
	  , J.[DatasetId]
	  , J.[UniversalId]
	  , J.[Shape]
	  , J.[Name]
	  , J.[Category]
	  , J.[SqlStatement]
	  , [StartDateParam] = CONVERT(DATETIME, CASE WHEN J.[DateParam2] IS NULL THEN NULL ELSE J.[DateParam1] END)
	  , [EndDateParam] = CONVERT(DATETIME, CASE WHEN J.[DateParam2] IS NULL THEN J.[DateParam1] ELSE J.[DateParam2] END)
	FROM [dbo].[UsageLog] AS L
	     CROSS APPLY OPENJSON(JSON_QUERY(L.Properties, '$.Context'))
		 WITH 
		    (
				[DatasetId] UNIQUEIDENTIFIER '$.DatasetId',
				[UniversalId] NVARCHAR(100) '$.DatasetQuery.UniversalId',
				[Shape] NVARCHAR(20) '$.Shape',
				[SqlStatement] NVARCHAR(MAX) '$.CompiledQuery',
				[DateParam1] DATETIMEOFFSET '$.Parameters[1].Value',
				[DateParam2] DATETIMEOFFSET '$.Parameters[2].Value',
				[Name] NVARCHAR(100) '$.DatasetQuery.Name',
				[Category] NVARCHAR(100) '$.DatasetQuery.Category'
			) AS J
	WHERE L.MessageTemplate = 'Compiled dataset execution context. Context:{@Context}'
)
, F AS
(
	SELECT
	    L.[Timestamp]
	  , L.[RequestId]
	  , J.[PatientCount]
	  , J.[RecordCount]
	FROM [dbo].[UsageLog] AS L
		 CROSS APPLY OPENJSON(L.Properties)
		 WITH 
		 (
			[PatientCount] INT '$.Patients',
			[RecordCount] INT '$.Records'
		 ) AS J
	WHERE L.MessageTemplate = 'Dataset complete. Patients:{Patients} Records:{Records}'
)
, E AS
(
	SELECT
	    L.[Timestamp]
	  , J.[Error]
	  , L.[RequestId]
	FROM [dbo].[UsageLog] AS L
		 CROSS APPLY OPENJSON(L.Properties)
		 WITH ([Error] NVARCHAR(MAX) '$.Error') AS J
	WHERE L.MessageTemplate = 'Failed to fetch dataset. QueryID:{QueryID} DatasetID:{DatasetID} Error:{Error}'
)		


SELECT 
	X.[Timestamp]
  , X.[User]
  , X.[SessionId]
  , Q.[DatasetId]
  , Q.[UniversalId]
  , Q.[Shape]
  , Q.[Name]
  , Q.[Category]
  , Q.[SqlStatement]
  , Q.[StartDateParam]
  , Q.[EndDateParam]
  , F.[PatientCount]
  , F.[RecordCount]
  , Success = CONVERT(BIT, CASE WHEN E.[Error] IS NULL THEN 1 ELSE 0 END)
  , [QueryStartTime] = X.[Timestamp]
  , [QueryEndTime] = ISNULL(E.[TimeStamp],F.[TimeStamp])
  , [QueryExecutionTimeInSeconds] = CONVERT(DECIMAL(18,1), DATEDIFF(MS, Q.[TimeStamp], ISNULL(E.[TimeStamp], F.[TimeStamp])) / 100.0, 1)
  , E.[Error]
  , X.[RequestId]
  , X.[ActionId]
  , X.[ActionName]
  , X.[RequestPath]
  , X.[SourceContext]
FROM X 
	 INNER JOIN Q
		ON X.RequestId = Q.RequestId
	 LEFT JOIN F
		ON X.RequestId = F.RequestId
	 LEFT JOIN E
		ON X.RequestId = E.RequestId
	 
GO
/****** Object:  View [dbo].[v_CountQuery]    Script Date: 11/22/2019 11:12:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [dbo].[v_CountQuery] AS

WITH X AS
(
	SELECT
		L.[Timestamp]
	  , L.[MessageTemplate]
	  , QueryId = CONVERT(UNIQUEIDENTIFIER, NULLIF(J.QueryId,''))
	  , L.[ActionId]
	  , L.[ActionName]
	  , L.[RequestId]
	  , L.[RequestPath]
	  , L.[SessionId]
	  , L.[SourceContext]
	  , L.[User]
	FROM [dbo].[UsageLog] AS L
	     CROSS APPLY OPENJSON(L.Properties)
		 WITH ([QueryId] NVARCHAR(100) '$.QueryId') AS J
	WHERE L.MessageTemplate = 'FullCount starting. DTO:{@DTO}'
)
, Q AS
(
	SELECT
	    L.[Timestamp]
	  , J.[SqlStatement]
	  , L.[RequestId]
	FROM [dbo].[UsageLog] AS L
	     CROSS APPLY OPENJSON(L.Properties)
		 WITH ([SqlStatement] NVARCHAR(MAX) '$.Sql') AS J
	WHERE L.MessageTemplate = 'CTE SqlStatement:{Sql}'
)
, F AS
(
	SELECT
	    L.[Timestamp]
	  , [PatientCount] = JSON_VALUE(L.Properties, '$.Cohort.Count')
	  , L.[RequestId]
	FROM [dbo].[UsageLog] AS L
	WHERE L.MessageTemplate = 'FullCount cohort retrieved. Cohort:{@Cohort}'
)
, E AS
(
	SELECT
	    L.[Timestamp]
	  , J.[Error]
	  , L.[RequestId]
	FROM [dbo].[UsageLog] AS L
		 CROSS APPLY OPENJSON(L.Properties)
		 WITH ([Error] NVARCHAR(MAX) '$.Error') AS J
	WHERE L.MessageTemplate = 'Failed to execute query. Error:{Error}'
)		


SELECT 
	X.[Timestamp]
  , X.[User]
  , X.[SessionId]
  , Q.[SqlStatement]
  , F.[PatientCount]
  , Success = CONVERT(BIT, CASE WHEN E.[Error] IS NULL THEN 1 ELSE 0 END)
  , [QueryStartTime] = X.[Timestamp]
  , [QueryEndTime] = ISNULL(E.[TimeStamp],F.[TimeStamp])
  , [QueryExecutionTimeInSeconds] = CONVERT(DECIMAL(18,1), DATEDIFF(MS, Q.[TimeStamp], ISNULL(E.[TimeStamp], F.[TimeStamp])) / 100.0, 1)
  , E.[Error]
  , X.[RequestId]
  , X.[ActionId]
  , X.[ActionName]
  , X.[RequestPath]
  , X.[SourceContext]
FROM X 
	 INNER JOIN Q
		ON X.RequestId = Q.RequestId
	 LEFT JOIN F
		ON X.RequestId = F.RequestId
	 LEFT JOIN E
		ON X.RequestId = E.RequestId
	 
GO
/****** Object:  View [dbo].[v_CountQueryDetail]    Script Date: 11/22/2019 11:12:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [dbo].[v_CountQueryDetail] AS

-- Top object level
WITH X1 AS
(
	SELECT
		L.[Timestamp]
	  , L.[MessageTemplate]
	  , Panels = JSON_QUERY(L.Properties, '$.DTO.Panels')
	  , L.[ActionId]
	  , L.[ActionName]
	  , L.[RequestId]
	  , L.[RequestPath]
	  , L.[SessionId]
	  , L.[SourceContext]
	  , L.[User]
	FROM [dbo].[UsageLog] AS L
	WHERE L.MessageTemplate = 'FullCount starting. DTO:{@DTO}'
)

-- Panel Level
, X2 AS
(
	SELECT
		[Timestamp]
	  , [MessageTemplate]
	  , [SubPanels]
	  , [DateFilter]
	  , [IncludePanel]
	  , [PanelIndex]
	  , [ActionId]
	  , [ActionName]
	  , [RequestId]
	  , [RequestPath]
	  , [SessionId]
	  , [SourceContext]
	  , [User]
	FROM X1 
	     CROSS APPLY OPENJSON(X1.Panels)
		 WITH 
		 (
			[SubPanels] NVARCHAR(MAX) AS JSON,
			[DateFilter] NVARCHAR(MAX) AS JSON,
		 	[IncludePanel] BIT '$.IncludePanel',
		 	[PanelIndex] INT '$.Index'
		 ) AS J1
)

-- SubPanel level
, X3 AS
(
	SELECT
		[Timestamp]
	  , [MessageTemplate]
	  , [IncludePanel]
	  , [PanelIndex]
	  , [DateFilterStart] = CAST(NULLIF(JSON_VALUE(X2.DateFilter, '$.Start.Date'),'0001-01-01T00:00:00') AS DATETIME)
	  , [DateFilterStartIncrement] = JSON_VALUE(X2.DateFilter, '$.Start.Increment')
	  , [DateFilterStartIncrementType] = JSON_VALUE(X2.DateFilter, '$.Start.DateIncrementType')
	  , [DateFilterEnd] = CAST(NULLIF(JSON_VALUE(X2.DateFilter, '$.End.Date'),'0001-01-01T00:00:00') AS DATETIME)
	  , [DateFilterEndIncrement] = JSON_VALUE(X2.DateFilter, '$.End.Increment')
	  , [DateFilterEndIncrementType] = JSON_VALUE(X2.DateFilter, '$.End.DateIncrementType')
	  , [SubPanelIndex]
	  , [PanelItems]
	  , [IncludeSubPanel]
	  , [MinimumCount]
	  , [JoinSequenceType]
	  , [JoinSequenceDateType]
	  , [JoinSequenceIncrement]
	  , [ActionId]
	  , [ActionName]
	  , [RequestId]
	  , [RequestPath]
	  , [SessionId]
	  , [SourceContext]
	  , [User]
	FROM X2 
	     CROSS APPLY OPENJSON(X2.SubPanels)
		 WITH 
		 (
			[PanelItems] NVARCHAR(MAX) AS JSON,
		 	[IncludeSubPanel] BIT '$.IncludeSubPanel',
		 	[SubPanelIndex] INT '$.Index',
			[MinimumCount] INT '$.MinimumCount',
			[JoinSequenceType] NVARCHAR(20) '$.JoinSequence.SequenceType',
			[JoinSequenceDateType] NVARCHAR(20) '$.JoinSequence.DateIncrementType',
			[JoinSequenceIncrement] NVARCHAR(20) '$.JoinSequence.Increment'
		 ) AS J1
)

-- PanelItem/Concept level
, X4 AS
(
	SELECT
		[Timestamp]
	  , [PanelIndex]
	  , [IncludePanel]
	  , [SubPanelIndex]
	  , [IncludeSubPanel]
	  , [MinimumCount]
	  , [PanelItemIndex]
	  , [ConceptId]
	  , [UniversalId]
	  , [NumericFilterType]
	  , [NumericFilterValue1]
	  , [NumericFilterValue2]
	  , [DateFilterStart]
	  , [DateFilterStartIncrement]
	  , [DateFilterStartIncrementType]
	  , [DateFilterEnd]
	  , [DateFilterEndIncrement]
	  , [DateFilterEndIncrementType]
	  , [JoinSequenceType]
	  , [JoinSequenceDateType]
	  , [JoinSequenceIncrement]
	  , [ActionId]
	  , [ActionName]
	  , [RequestId]
	  , [RequestPath]
	  , [SessionId]
	  , [SourceContext]
	  , [User]
	FROM X3
	     CROSS APPLY OPENJSON(X3.PanelItems)
		 WITH 
		 (
			[ConceptId] UNIQUEIDENTIFIER '$.Resource.Id',
		 	[UniversalId] NVARCHAR(100) '$.Resource.UniversalId',
			[PanelItemIndex] INT '$.Index',
			[NumericFilterType] NVARCHAR(100) '$.NumericFilter.FilterType',
			[NumericFilterValue1] NVARCHAR(100) '$.NumericFilter.Filter[0]',
			[NumericFilterValue2] NVARCHAR(100) '$.NumericFilter.Filter[1]'
		 ) AS J1
)

SELECT
	  [Timestamp]
	, [User]
	, [SessionId]
	, [PanelIndex]
	, [IncludePanel]
	, [SubPanelIndex]
	, [IncludeSubPanel]
	, [MinimumCount]
	, [PanelItemIndex]
	, [ConceptId]
	, [UniversalId]
	, [NumericFilterType]
	, [NumericFilterValue1]
	, [NumericFilterValue2]
	, [DateFilterStart]
	, [DateFilterStartIncrement]
	, [DateFilterStartIncrementType]
	, [DateFilterEnd]
	, [DateFilterEndIncrement]
	, [DateFilterEndIncrementType]
	, [JoinSequenceType]
	, [JoinSequenceDateType]
	, [JoinSequenceIncrement]
	, [RequestId]
    , [ActionId]
    , [ActionName]
    , [RequestPath]
    , [SourceContext]
FROM X4
GO
/****** Object:  View [dbo].[v_ConceptChildren]    Script Date: 11/22/2019 11:12:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE VIEW [dbo].[v_ConceptChildren] AS

	SELECT
	    [Timestamp]
	  , [User]
	  , [SessionId]
	  , J.Concept
	  , [RequestId]
	  , [ActionId]
	  , [ActionName]
	  , [RequestPath]
	  , [SourceContext]
	FROM [dbo].[UsageLog] AS L
		 CROSS APPLY OPENJSON(L.Properties)
	WITH (Concept UNIQUEIDENTIFIER '$.Parent') AS J
	WHERE L.MessageTemplate = 'Getting child concepts. Parent:{Parent}'
GO
/****** Object:  View [dbo].[v_ConceptSearch]    Script Date: 11/22/2019 11:12:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE VIEW [dbo].[v_ConceptSearch] AS

WITH X AS
(
	SELECT
	    [Timestamp]
	  , [User]
	  , [SessionId]
	  , J.Hints
	  , Properties
	  , [RequestId]
	  , [ActionId]
	  , [ActionName]
	  , [RequestPath]
	  , [SourceContext]
	FROM [dbo].[UsageLog] AS L
		 CROSS APPLY OPENJSON(L.Properties)
		 WITH (Hints NVARCHAR(MAX) AS JSON) AS J
	WHERE L.MessageTemplate = 'Found hints. Hints:{Hints}'
)

SELECT
    [Timestamp]
  , [User]
  , [SessionId]
  , J.SearchTerm
  , HintsFound = (SELECT COUNT(*) FROM X CROSS APPLY OPENJSON(X.Hints) WHERE X.RequestId = L.RequestId)
  , [RequestId]
  , [ActionId]
  , [ActionName]
  , [RequestPath]
  , [SourceContext]
FROM [dbo].[UsageLog] AS L
	 CROSS APPLY OPENJSON(L.Properties)
WITH (SearchTerm NVARCHAR(100) '$.Terms') AS J
WHERE L.MessageTemplate = 'Searching hints by terms. Terms:{Terms}'

GO
/****** Object:  View [dbo].[v_Login]    Script Date: 11/22/2019 11:12:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE VIEW [dbo].[v_Login] AS

WITH X AS
(
	SELECT
	    [Timestamp]
	  , [User]
	  , [SessionId]
	  , [Attestation]
	  , [RequestId]
	  , [ActionId]
	  , [ActionName]
	  , [RequestPath]
	  , [SourceContext]
	FROM [dbo].[UsageLog] AS L
		 CROSS APPLY OPENJSON(L.Properties)
		 WITH (Attestation NVARCHAR(MAX) AS JSON) AS J
	WHERE L.MessageTemplate = 'Created Access Token. Attestation:{@Attestation} Token:{Token}'
)

SELECT
    [Timestamp]
  , [User]
  , [SessionId]
  , [SessionType]
  , [IsIdentified]
  , [DocExpirationDate] = NULLIF([DocExpirationDate],'0001-01-01 00:00:00.0000000')
  , [DocInstitution]
  , [DocTitle]
  , [RequestId]
  , [ActionId]
  , [ActionName]
  , [RequestPath]
  , [SourceContext]
FROM X CROSS APPLY OPENJSON(X.Attestation)
WITH (
		SessionType NVARCHAR(20) '$.SessionType',
		IsIdentified BIT '$.IsIdentified',
		DocExpirationDate DATETIME2 '$.Documentation.ExpirationDate',
		DocInstitution NVARCHAR(100) '$.Documentation.Institution',
		DocTitle NVARCHAR(100) '$.Documentation.Title'
	 ) AS J

GO
/****** Object:  View [dbo].[v_UnauthorizedLogin]    Script Date: 11/22/2019 11:12:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






CREATE VIEW [dbo].[v_UnauthorizedLogin] AS

WITH X AS 
(
	SELECT
		[Timestamp]
	  , [BeginIdx] = CHARINDEX('"Error": "', L.Properties) + LEN('"Error": "')
	  , [EndIdx] = CHARINDEX('is not a Leaf user', L.Properties)
	  , [SessionId]
	  , L.Properties
	  , [RequestId]
	  , [ActionId]
	  , [ActionName]
	  , [RequestPath]
	  , [SourceContext]
	FROM [dbo].[UsageLog] AS L
	WHERE L.MessageTemplate = 'User is not authorized to use Leaf. Error:{Error}'
)

SELECT
    [Timestamp]
  , [User] = TRIM(SUBSTRING([Properties], [BeginIdx], [EndIdx] - [BeginIdx]))
  , [SessionId]
  , [RequestId]
  , [ActionId]
  , [ActionName]
  , [RequestPath]
  , [SourceContext]
FROM X

GO
/****** Object:  View [dbo].[v_QuerySave]    Script Date: 11/22/2019 11:12:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






CREATE VIEW [dbo].[v_QuerySave] AS

WITH X AS
(
	SELECT
	    [Timestamp]
	  , [User]
	  , [SessionId]
	  , [RequestId]
	  , [ActionId]
	  , [ActionName]
	  , [RequestPath]
	  , [SourceContext]
	FROM [dbo].[UsageLog] AS L
	WHERE L.MessageTemplate = 'Saving query. Id:{Id} Ast:{Ast}'
)
, S AS
(
	SELECT
	    [Timestamp]
	  , [User]
	  , [SessionId]
	  , J.[QueryId]
	  , J.[UniversalID]
	  , J.[Name]
	  , J.[Category]
	  , J.[Version]
	  , [RequestId]
	  , [ActionId]
	  , [ActionName]
	  , [RequestPath]
	  , [SourceContext]
	FROM [dbo].[UsageLog] AS L
	     CROSS APPLY OPENJSON(L.Properties)
		 WITH 
		 (
			[QueryId] UNIQUEIDENTIFIER '$.Query',
			[UniversalId] NVARCHAR(100) '$.Payload.UniversalId.Value',
			[Version] INT '$.Payload.Ver',
			[Name] NVARCHAR(100) '$.Payload.Name',
			[Category] NVARCHAR(100) '$.Payload.Category'
		 ) AS J
	WHERE L.MessageTemplate = 'Saving query. Query:{Query} Payload:{@Payload}'
)
, E AS
(
	SELECT
	    [Timestamp]
	  , [User]
	  , [SessionId]
	  , [Error] = JSON_VALUE(L.Properties, '$.Error')
	  , [RequestId]
	  , [ActionId]
	  , [ActionName]
	  , [RequestPath]
	  , [SourceContext]
	FROM [dbo].[UsageLog] AS L
	WHERE L.MessageTemplate = 'Failed to save query. Query:{@Query} Code:{Code} Error:{Error}'
)

SELECT 
	X.[Timestamp]
  , X.[User]
  , X.[SessionId]
  , S.[QueryId]
  , S.[UniversalId]
  , S.[Name]
  , S.[Category]
  , S.[Version]
  , Success = CONVERT(BIT, CASE WHEN E.[Error] IS NULL THEN 1 ELSE 0 END)
  , E.[Error]
  , X.[RequestId]
  , X.[ActionId]
  , X.[ActionName]
  , X.[RequestPath]
  , X.[SourceContext]
FROM X 
	 INNER JOIN S
		ON X.RequestId = S.RequestId
	 LEFT JOIN E
		ON X.RequestId = E.RequestId

GO
/****** Object:  View [dbo].[v_QueryDelete]    Script Date: 11/22/2019 11:12:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





CREATE VIEW [dbo].[v_QueryDelete] AS

WITH X AS 
(
	SELECT
		[Timestamp]
	  , [User]
	  , [SessionId]
	  , [UniversalId] = J.[Query]
	  , [BeginIdx] = CHARINDEX('urn:leaf:query:', [Query]) + LEN('urn:leaf:query:')
	  , [EndIdx] = CHARINDEX(':', REVERSE([Query]))
	  , J.[Force]
	  , [RequestId]
	  , [ActionId]
	  , [ActionName]
	  , [RequestPath]
	  , [SourceContext]
	FROM [dbo].[UsageLog] AS L
		 CROSS APPLY OPENJSON(L.Properties)
		 WITH (
			[Query] NVARCHAR(100) '$.Query',
			[Force] BIT '$.Force'
		 ) AS J
	WHERE L.MessageTemplate = 'Deleting query. Query:{Query} Force:{Force}'
)

SELECT 
	[Timestamp]
  , [User]
  , [SessionId]
  , [UniversalId]
  , [QueryId] = TRIM(SUBSTRING([UniversalId], [BeginIdx], LEN([UniversalId]) - [BeginIdx] - [EndIdx] + 1))
  , [Force]
  , [RequestId]
  , [ActionId]
  , [ActionName]
  , [RequestPath]
  , [SourceContext]
FROM X
GO
/****** Object:  View [dbo].[v_DemographicsDatasetQuery]    Script Date: 11/22/2019 11:12:37 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






CREATE VIEW [dbo].[v_DemographicsDatasetQuery] AS

WITH X AS
(
	SELECT
		L.[Timestamp]
	  , L.[MessageTemplate]
	  , L.[ActionId]
	  , L.[ActionName]
	  , L.[RequestId]
	  , L.[RequestPath]
	  , L.[SessionId]
	  , L.[SourceContext]
	  , L.[User]
	FROM [dbo].[UsageLog] AS L
	WHERE L.MessageTemplate = 'Demographics starting. QueryRef:{QueryRef}'
)
, Q AS
(
	SELECT
	    L.[Timestamp]
	  , L.Properties
	  , L.[RequestId]
	  , J.[Shape]
	  , J.[SqlStatement]
	FROM [dbo].[UsageLog] AS L
	     CROSS APPLY OPENJSON(JSON_QUERY(L.Properties, '$.Context'))
		 WITH 
		    (
				[Shape] NVARCHAR(20) '$.Shape',
				[SqlStatement] NVARCHAR(MAX) '$.CompiledQuery'
			) AS J
	WHERE L.MessageTemplate = 'Compiled demographic execution context. Context:{@Context}'
)
, F AS
(
	SELECT
	    L.[Timestamp]
	  , L.[RequestId]
	  , J.[ExportedCount]
	  , J.[TotalAggregatedCount]
	FROM [dbo].[UsageLog] AS L
		 CROSS APPLY OPENJSON(L.Properties)
		 WITH 
		 (
			[ExportedCount] INT '$.Exported',
			[TotalAggregatedCount] INT '$.Total'
		 ) AS J
	WHERE L.MessageTemplate = 'Demographics complete. Exported:{Exported} Total:{Total}'
)
, E AS
(
	SELECT
	    L.[Timestamp]
	  , J.[Error]
	  , L.[RequestId]
	FROM [dbo].[UsageLog] AS L
		 CROSS APPLY OPENJSON(L.Properties)
		 WITH ([Error] NVARCHAR(MAX) '$.Error') AS J
	WHERE L.MessageTemplate = 'Failed to fetch dataset. QueryID:{QueryID} DatasetID:{DatasetID} Error:{Error}'
)		


SELECT 
	X.[Timestamp]
  , X.[User]
  , X.[SessionId]
  , Q.[Shape]
  , F.[ExportedCount]
  , F.[TotalAggregatedCount]
  , Success = CONVERT(BIT, CASE WHEN E.[Error] IS NULL THEN 1 ELSE 0 END)
  , [QueryStartTime] = X.[Timestamp]
  , [QueryEndTime] = ISNULL(E.[TimeStamp],F.[TimeStamp])
  , [QueryExecutionTimeInSeconds] = CONVERT(DECIMAL(18,1), DATEDIFF(MS, Q.[TimeStamp], ISNULL(E.[TimeStamp], F.[TimeStamp])) / 100.0, 1)
  , E.[Error]
  , X.[RequestId]
  , X.[ActionId]
  , X.[ActionName]
  , X.[RequestPath]
  , X.[SourceContext]
FROM X 
	 INNER JOIN Q
		ON X.RequestId = Q.RequestId
	 LEFT JOIN F
		ON X.RequestId = F.RequestId
	 LEFT JOIN E
		ON X.RequestId = E.RequestId
		
/****** Object:  View [dbo].[v_ExportREDCap]    Script Date: 3/5/2020 4:32:55 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO







CREATE VIEW [dbo].[v_ExportREDCap] AS

WITH X AS
(
	SELECT
	    [Timestamp]
	  , [User]
	  , [SessionId]
	  , [RequestId]
	  , [ActionId]
	  , [ActionName]
	  , [RequestPath]
	  , [SourceContext]
	FROM [dbo].[UsageLog] AS L
	WHERE L.MessageTemplate = 'Creating REDCap Project. Project:{Project}'
)

SELECT
    [Timestamp]
  , [User]
  , [SessionId]
  , [RequestId]
  , [ActionId]
  , [ActionName]
  , [RequestPath]
  , [SourceContext]
FROM X

GO		
