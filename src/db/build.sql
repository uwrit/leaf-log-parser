USE [LeafLog]
GO
/****** Object:  Table [dbo].[UsageLog]    Script Date: 7/26/2019 4:36:02 PM ******/
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
/****** Object:  View [dbo].[v_DatasetQuery]    Script Date: 7/26/2019 4:36:03 PM ******/
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
	  , Context = JSON_QUERY(L.Properties, '$.Context')
	  , L.[Properties]
	  , L.[ActionId]
	  , L.[ActionName]
	  , L.[RequestId]
	  , L.[RequestPath]
	  , L.[SessionId]
	  , L.[SourceContext]
	  , L.[User]
	FROM [LeafLog].[dbo].[UsageLog] AS L
	WHERE L.MessageTemplate = 'Compiled dataset execution context. Context:{@Context}'
)

SELECT 
	[Timestamp]
  , [DatasetId] = JSON_VALUE(X.Context, '$.DatasetId')
  , [Shape] = JSON_VALUE(X.Context, '$.Shape')
  , [CompiledQuery] = JSON_VALUE(X.Context, '$.CompiledQuery')
  , [SessionId]
  , [RequestId]
  , [ActionId]
  , [ActionName]
  , [RequestPath]
  , [SourceContext]
  , [User]
FROM X
GO
/****** Object:  View [dbo].[v_CountQuery]    Script Date: 7/26/2019 4:36:03 PM ******/
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
	  , Cohort = JSON_QUERY(L.Properties, '$.Cohort')
	  , L.[Properties]
	  , L.[ActionId]
	  , L.[ActionName]
	  , L.[RequestId]
	  , L.[RequestPath]
	  , L.[SessionId]
	  , L.[SourceContext]
	  , L.[User]
	FROM [LeafLog].[dbo].[UsageLog] AS L
	WHERE L.RequestPath = '/api/cohort/count'
		  AND L.MessageTemplate = 'FullCount cohort retrieved. Cohort:{@Cohort}'
)

SELECT 
	[Timestamp]
  , [SqlStatement] = JSON_VALUE(X.Cohort, '$.SqlStatements[0]')
  , [PatientCount] = JSON_VALUE(X.Cohort, '$.Count')
  , [SessionId]
  , [RequestId]
  , [ActionId]
  , [ActionName]
  , [RequestPath]
  , [SourceContext]
  , [User]
FROM X
GO
/****** Object:  View [dbo].[v_CountQueryDetail]    Script Date: 7/26/2019 4:36:03 PM ******/
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
	  , Requested = JSON_QUERY(L.Properties, '$.Context.Requested')
	  , L.[Properties]
	  , L.[ActionId]
	  , L.[ActionName]
	  , L.[RequestId]
	  , L.[RequestPath]
	  , L.[SessionId]
	  , L.[SourceContext]
	  , L.[User]
	FROM [LeafLog].[dbo].[UsageLog] AS L
	WHERE L.RequestPath = '/api/cohort/count'
		  AND L.MessageTemplate = 'FullCount panel validation context. Context:{@Context}'
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
	  , [Properties]
	  , [ActionId]
	  , [ActionName]
	  , [RequestId]
	  , [RequestPath]
	  , [SessionId]
	  , [SourceContext]
	  , [User]
	FROM X1 
	     CROSS APPLY OPENJSON(X1.Requested)
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
FROM X4
GO
ALTER TABLE [dbo].[UsageLog] ADD  CONSTRAINT [DF_UsageLog_Id]  DEFAULT (newsequentialid()) FOR [Id]
GO
