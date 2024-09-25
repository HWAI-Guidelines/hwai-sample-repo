-- All content, trademarks, and data on this document are the property of Healthworks Analytics, LLC and are protected by applicable intellectual property laws. Unauthorized use, reproduction, or distribution of this material is strictly prohibited.
/****** Script for ET New TechStack  ******/

-- STEP:1
-- CREATE BASE Trends_CW_withpredictions TABLE IN STAGING SERVER

USE [HWai_Dashboards]
GO
/****** Object:  Table [dbo].[TrendsCwWithPredictions]    Script Date: 17-04-2023 18:14:54 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TrendsCwWithPredictions](
	[Contract_ID] [varchar](50) NULL,
	[County] [varchar](50) NULL,
	[EGHP] [varchar](50) NULL,
	[FIPS_State_County_Code] [varchar](50) NULL,
	[Month] [varchar](50) NULL,
	[Offers_Part_D] [varchar](50) NULL,
	[Other Dual Full Medicaid Benefit] [float] NULL,
	[Parent_Organization] [varchar](500) NULL,
	[Plan_ID] [varchar](50) NULL,
	[Plan_Name] [varchar](500) NULL,
	[Plan_Type] [varchar](50) NULL,
	[QDWI] [float] NULL,
	[QI] [float] NULL,
	[QMB only] [float] NULL,
	[QMB plus Full Medicaid Benefits] [float] NULL,
	[SLMB only] [float] NULL,
	[SLMB plus Full Medicaid Benefits] [float] NULL,
	[SNP_Eligibles] [float] NULL,
	[SNP_Plan] [varchar](50) NULL,
	[SSA_State_County_Code] [varchar](50) NULL,
	[Special_Needs_Plan_Type] [varchar](50) NULL,
	[State] [varchar](50) NULL,
	[Year] [int] NULL,
	[enrollments] [float] NULL,
	[Key_Cp_Id] [varchar](50) NULL,
	[ma_eligibles] [varchar](50) NULL,
	[Stability_Index] [float] NULL,
	[Prediction] [varchar](50) NULL,
	[Status] [varchar](500) NULL,
	[IsTerminated] [int] NULL,
	[IsOutofArea] [int] NULL,
	[IsStarEnr] [int] NULL
) ON [PRIMARY]
GO

-- STEP:2
-- CREATE CLIENTORGDETAILS TABLE
USE [HWai_Dashboards]
GO
/****** Object:  Table [dbo].[ClientOrgDetails]    Script Date: 17-04-2023 22:46:11 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ClientOrgDetails](
	[ClientID] [int] IDENTITY(1,1) NOT NULL,
	[ClientName] [varchar](50) NULL,
	[IsPredicted] [bit] NULL,
 CONSTRAINT [PK_ClientOrgDetails] PRIMARY KEY CLUSTERED 
(
	[ClientID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

-- STEP3
-- CREATE UserClientDetails TABLE

USE [HWai_Dashboards]
GO
/****** Object:  Table [dbo].[UserClientDetails]    Script Date: 17-04-2023 23:15:25 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[UserDetails](
	[UserId] [varchar](100) PRIMARY KEY,
	[UserName] [varchar](50) NULL,
	[FirstName] [varchar](50) NULL,
	[LastName] [varchar](50) NULL,
	[ClientID] [int] NULL FOREIGN KEY REFERENCES ClientOrgDetails(ClientID)
	)

-- STEP:4
-- CREATE EnrollmentTrendsState TABLE

USE [HWai_Dashboards]
GO
/****** Object:  Table [dbo].[EnrollmentTrendsState]    Script Date: 17-04-2023 23:38:04 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EnrollmentTrendsState](
	[StateID] [int] IDENTITY(1,1) NOT NULL,
	[StateName] [varchar](50) NULL,
	[StateCode] [varchar](20) NULL,
 CONSTRAINT [PK_EnrollmentTrendsState] PRIMARY KEY CLUSTERED 
(
	[StateID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

-- STEP:5
-- CREATE EnrollmentTrendsSalesRegion TABLE
USE [HWai_Dashboards]
GO
/****** Object:  Table [dbo].[EnrollmentTrendsSalesRegion]    Script Date: 17-04-2023 23:56:14 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EnrollmentTrendsSalesRegion](
	[SalesRegionID] [int] IDENTITY(1,1) NOT NULL,
	[SalesRegionName] [varchar](50) NOT NULL
 CONSTRAINT [PK_EnrollmentTrendsDevotedSalesRegion] PRIMARY KEY CLUSTERED 
(
	[SalesRegionID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


-- STEP:5
-- CREATE EnrollmentTrendsCounty TABLE
USE [HWai_Dashboards]
GO
/****** Object:  Table [dbo].[EnrollmentTrendsCounty]    Script Date: 17-04-2023 23:51:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EnrollmentTrendsCounty](
	[CountyID] [int] IDENTITY(1,1) NOT NULL,
	[County] [varchar](50) NULL,
	[CountyCode] [varchar](10) NULL,
 CONSTRAINT [PK_EnrollmentTrendsCounty] PRIMARY KEY CLUSTERED 
(
	[CountyID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


-- STEP:7
-- CREATE EnrollmentTrendsIndGroupPlans TABLE
/****** Object:  Table [dbo].[EnrollmentTrendsIndGroupPlans]    Script Date: 17-04-2023 23:51:01 ******/
USE [HWai_Dashboards]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EnrollmentTrendsIndGroupPlans](
	[IndGroupPlansID] [int] IDENTITY(1,1) NOT NULL,
	[IndGroupPlans] [varchar](50) NULL,
 CONSTRAINT [PK_EnrollmentTrendsIndGroupPlans] PRIMARY KEY CLUSTERED 
(
	[IndGroupPlansID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO


-- STEP:8
-- CREATE EnrollmentTrendsMAPDP TABLE

/****** Object:  Table [dbo].[EnrollmentTrendsMAPDP]    Script Date: 17-04-2023 23:51:01 ******/
USE [HWai_Dashboards]
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EnrollmentTrendsMAPDP](
	[MAPDPID] [int] IDENTITY(1,1) NOT NULL,
	[MAPDP] [varchar](50) NULL,
 CONSTRAINT [PK_EnrollmentTrendsMAPDP] PRIMARY KEY CLUSTERED 
(
	[MAPDPID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO



-- STEP:9
-- CREATE EnrollmentTrendsOrganization TABLE

/****** Object:  Table [dbo].[EnrollmentTrendsOrganization]    Script Date: 17-04-2023 23:51:01 ******/
USE [HWai_Dashboards]
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EnrollmentTrendsOrganization](
	[OrganizationID] [int] IDENTITY(1,1) NOT NULL,
	[OrganizationName] [varchar](100) NULL,
 CONSTRAINT [PK_EnrollmentTrendsOrganization] PRIMARY KEY CLUSTERED 
(
	[OrganizationID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO



-- STEP:9
-- CREATE EnrollmentTrendsPlanType TABLE

/****** Object:  Table [dbo].[EnrollmentTrendsPlanType]    Script Date: 17-04-2023 23:51:01 ******/
USE [HWai_Dashboards]
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EnrollmentTrendsPlanType](
	[PlanTypeID] [int] IDENTITY(1,1) NOT NULL,
	[PlanType] [varchar](50) NULL,
 CONSTRAINT [PK_EnrollmentTrendsPlanType] PRIMARY KEY CLUSTERED 
(
	[PlanTypeID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

-- STEP:10
-- CREATE EnrollmentTrendsSNP TABLE

/****** Object:  Table [dbo].[EnrollmentTrendsSNP]    Script Date: 17-04-2023 23:51:01 ******/
USE [HWai_Dashboards]
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[EnrollmentTrendsSNP](
	[SNPID] [int] IDENTITY(1,1) NOT NULL,
	[SNP] [varchar](50) NULL,
 CONSTRAINT [PK_EnrollmentTrendsSNP] PRIMARY KEY CLUSTERED 
(
	[SNPID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

-- STEP:11
-- CREATE TERMINATED PLANS TABLE

/****** Object:  Table [dbo].[EnrollmentTrendsSNP]    Script Date: 17-04-2023 23:51:01 ******/
USE [HWai_Dashboards]
CREATE TABLE [dbo].[EnrollmentTrendsTerminatedPlans](
	[BidId] [varchar](30) PRIMARY KEY,
	[IsTerminated] [INT] NULL
	)