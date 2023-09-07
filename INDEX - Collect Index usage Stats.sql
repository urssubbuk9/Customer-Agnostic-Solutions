SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
--=============================================
-- You may alter this code for your own *non-commercial* purposes. You may
-- republish altered code as long as you give due credit.
--   
-- THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF 
-- ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
-- TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
-- PARTICULAR PURPOSE.
--
-- =============================================
-- Description:	This process will keep index usage stats in a user table to survive server restarts
--
-- Assupmtions:	This process will run on a schedule 
--				Usage between the last time the process run and the server restart will be lost
--
-- Change Log:	14/11/2013 RAG 	- Created
--              04/12/2013 RAG 	- Added column [LastDataCollectionTime]
--				
-- =============================================

SET NOCOUNT ON

DECLARE @dbname					SYSNAME			= 'WideWorldImporters'	
DECLARE @sql_server_boot_time	DATETIME2(0)	= (SELECT MAX(sqlserver_start_time) FROM sys.dm_os_sys_info)

IF OBJECT_ID('tempdb..#result')		IS NOT NULL DROP TABLE #result
IF OBJECT_ID('tempdb..#databases')	IS NOT NULL DROP TABLE #databases

CREATE TABLE #result 
	( ID						INT IDENTITY(1,1)	NOT NULL
	, [index_columns]			NVARCHAR(4000)		NULL
	, included_columns			NVARCHAR(4000)		NULL
	, filter					NVARCHAR(4000)		NULL
	, dbname					SYSNAME	COLLATE DATABASE_DEFAULT	NULL
	, tableName					SYSNAME	COLLATE DATABASE_DEFAULT	NULL
	, index_id					INT					NULL
	, partition_number			INT					NULL
	, index_name				SYSNAME	COLLATE DATABASE_DEFAULT	NULL
	, index_type				SYSNAME				NULL
	, is_primary_key			VARCHAR(3)			NULL
	, is_unique					VARCHAR(3)			NULL
	, is_disabled				VARCHAR(3)			NULL
	, row_count					BIGINT 				NULL
	, reserved_MB				DECIMAL(10,2)		NULL
	, size_MB					DECIMAL(10,2)		NULL
	, fill_factor				TINYINT				NULL
	, user_seeks				BIGINT				NULL
	, user_scans				BIGINT				NULL
	, user_lookups				BIGINT				NULL
	, user_updates				BIGINT				NULL
	, filegroup_desc			SYSNAME				NULL
	, data_compression_desc		SYSNAME				NULL		
)

IF OBJECT_ID('dbo.IndexUsageStats') IS NULL BEGIN 
	CREATE TABLE dbo.IndexUsageStats 
		( ID						INT IDENTITY(1,1) 					NOT NULL PRIMARY KEY
		, [FirstBootupDateTime]		DATETIME2(0)						NULL
		, [LastBootupDateTime]		DATETIME2(0)						NULL
		, [LastDataCollectionTime]	DATETIME2(0)						NULL
		, [index_columns]			NVARCHAR(4000)						NULL	
		, included_columns			NVARCHAR(4000)						NULL
		, filter					NVARCHAR(4000)						NULL
		, dbname					SYSNAME	COLLATE DATABASE_DEFAULT	NULL	
		, tableName					SYSNAME	COLLATE DATABASE_DEFAULT	NULL	
		, index_id					INT									NULL
		, partition_number			INT									NULL
		, index_name				SYSNAME	COLLATE DATABASE_DEFAULT	NULL
		, index_type				SYSNAME								NULL
		, is_primary_key			VARCHAR(3)							NULL
		, is_unique					VARCHAR(3)							NULL
		, is_disabled				VARCHAR(3)							NULL
		, row_count					BIGINT 								NULL
		, reserved_MB				DECIMAL(10,2)						NULL
		, size_MB					DECIMAL(10,2)						NULL
		, fill_factor				TINYINT								NULL
		, user_seeks				BIGINT								NULL
		, user_scans				BIGINT								NULL
		, user_lookups				BIGINT								NULL
		, user_updates				BIGINT								NULL
		, user_seeks_cumulative		BIGINT								NULL
		, user_scans_cumulative		BIGINT								NULL
		, user_lookups_cumulative	BIGINT								NULL
		, user_updates_cumulative	BIGINT								NULL
		, filegroup_desc			SYSNAME								NULL
		, data_compression_desc		SYSNAME								NULL)

END

CREATE TABLE #databases 
	( ID			INT IDENTITY
	, dbname		SYSNAME NOT NULL)
		
DECLARE @sqlString	NVARCHAR(MAX)
		, @countDB	INT = 1
		, @numDB	INT

INSERT INTO #databases (dbname)
	SELECT name 
		FROM sys.databases 
		WHERE [name] NOT IN ('model', 'tempdb') 
			AND state = 0 
			AND name LIKE ISNULL(@dbname, name)
		ORDER BY name ASC		

SET @numDB = @@ROWCOUNT

WHILE @countDB <= @numDB BEGIN
	
	SELECT @dbname = dbname from #databases WHERE ID = @countDB
	SET @sqlString = N'
		
		USE ' + QUOTENAME(@dbname) + N'	
			
		IF OBJECT_ID(''tempdb..#index_usage_stats'') IS NOT NULL DROP TABLE #index_usage_stats

		SELECT * 
			INTO #index_usage_stats
			FROM sys.dm_db_index_usage_stats WITH (NOLOCK)
			WHERE database_id = DB_ID()
			
		-- Get indexes 
		SELECT  			
				STUFF( (SELECT N'', '' + QUOTENAME(c.name) + CASE WHEN ixc.is_descending_key = 1 THEN N'' DESC'' ELSE N'' ASC'' END +
								CASE WHEN c.is_nullable = 1 THEN '' (NULL)'' ELSE '''' END
							FROM sys.index_columns as ixc 
								INNER JOIN sys.columns as c
									ON ixc.object_id = c.object_id
										AND ixc.column_id = c.column_id
							WHERE ixc.object_id = ix.object_id 
								AND ixc.index_id = ix.index_id 
								AND ixc.is_included_column = 0
							ORDER BY ixc.index_column_id
							FOR XML PATH ('''')), 1,2,'''')
					
				, STUFF( (SELECT N'', '' + QUOTENAME(c.name)
								FROM sys.index_columns as ixc 
									INNER JOIN sys.columns as c
										ON ixc.object_id = c.object_id
											AND ixc.column_id = c.column_id
								WHERE ixc.object_id = ix.object_id 
									AND ixc.index_id = ix.index_id 
									AND ixc.is_included_column = 1
								ORDER BY c.name
								FOR XML PATH ('''')), 1,2,'''')
				, ix.filter_definition
				, QUOTENAME(DB_NAME()) AS DatabaseName
				, QUOTENAME(SCHEMA_NAME(o.schema_id)) + ''.'' + QUOTENAME(o.name) AS TableName
				, ix.index_id AS index_id 
				, p.partition_number
				, ix.name
				, ix.type_desc
				, CASE WHEN ix.is_primary_key = 1	THEN ''Yes'' WHEN ix.is_primary_key = 0 THEN  ''No'' END AS is_primary_key
				, CASE WHEN ix.is_unique = 1		THEN ''Yes'' WHEN ix.is_unique = 0		THEN  ''No'' END AS is_unique
				, CASE WHEN ix.is_disabled = 1		THEN ''Yes'' WHEN ix.is_disabled = 0	THEN  ''No'' END AS is_disabled
				, ps.row_count
				, ( ps.reserved_page_count * 8 ) / 1024. 
				, ( ps.used_page_count * 8 ) / 1024. 
				, ix.fill_factor
				, ius.user_seeks
				, ius.user_scans
				, ius.user_lookups
				, ius.user_updates
				, d.name AS filegroup_desc
				, p.data_compression_desc
			FROM sys.indexes AS ix
				INNER JOIN sys.objects AS o 
					ON o.object_id = ix.object_id
				INNER JOIN sys.data_spaces AS d
					ON d.data_space_id = ix.data_space_id
				INNER JOIN sys.dm_db_partition_stats AS ps
					ON ps.object_id = ix.object_id 
						AND ps.index_id = ix.index_id
				INNER JOIN sys.partitions AS p 
					ON p.object_id = ix.object_id 
						AND p.index_id = ix.index_id
						AND p.partition_number = ps.partition_number
				LEFT JOIN #index_usage_stats AS ius
					ON ius.object_id = ix.object_id 
						AND ius.index_id = ix.index_id
						AND ius.database_id = DB_ID()
			WHERE o.type IN (''U'', ''V'')
				AND o.is_ms_shipped <> 1
				AND ix.type > 0	'

	--SELECT @sqlString

	INSERT INTO #result (index_columns, included_columns, filter, dbname, tableName, index_id, partition_number
						, index_name, index_type, is_primary_key, is_unique, is_disabled, row_count, reserved_MB
						, size_MB, fill_factor, user_seeks, user_scans, user_lookups, user_updates, filegroup_desc, data_compression_desc)	
	EXEC sp_executesql 	@stmt = @sqlString

	SET @countDB = @countDB + 1
END

--=========================================================
-- Time to update the table with the information collected.
--=========================================================

-- The index do not exist in the table
INSERT INTO dbo.IndexUsageStats (
		FirstBootupDateTime, LastBootupDateTime, LastDataCollectionTime, index_columns, included_columns, filter, dbname, tableName, index_id, partition_number, index_name
		, index_type, is_primary_key, is_unique, is_disabled, row_count, reserved_MB, size_MB, fill_factor
		, user_seeks, user_scans, user_lookups, user_updates
		, user_seeks_cumulative, user_scans_cumulative, user_lookups_cumulative, user_updates_cumulative
		, filegroup_desc, data_compression_desc)
SELECT @sql_server_boot_time, @sql_server_boot_time, GETDATE(), s.[index_columns], s.included_columns, s.filter, s.dbname, s.tableName, s.index_id, s.partition_number, s.index_name
		, s.index_type, s.is_primary_key, s.is_unique, s.is_disabled, s.row_count, s.reserved_MB, s.size_MB, s.fill_factor
		, s.user_seeks, s.user_scans, s.user_lookups, s.user_updates
		, 0, 0, 0, 0
		, s.filegroup_desc, s.data_compression_desc
	FROM #result AS s
	LEFT JOIN dbo.IndexUsageStats AS t
	ON s.dbname						= t.dbname			COLLATE DATABASE_DEFAULT		
		AND s.tableName				= t.tableName		COLLATE DATABASE_DEFAULT	
		AND s.index_name			= t.index_name		COLLATE DATABASE_DEFAULT
		AND s.index_id				= t.index_id
		AND s.partition_number		= t.partition_number	
	WHERE t.ID IS NULL


-- if the server has restarted (LastBootupDateTime < @sql_server_boot_time), 
-- we do the following 
--		- Add up the existing current and totals 
--		- Reset the LastBootupDateTime, so the next step can update the current values

UPDATE dbo.IndexUsageStats
	SET user_seeks_cumulative		= user_seeks_cumulative	 + user_seeks	-- Add up all values
		, user_scans_cumulative		= user_scans_cumulative	 + user_scans	-- Add up all values
		, user_lookups_cumulative	= user_lookups_cumulative + user_lookups	-- Add up all values
		, user_updates_cumulative	= user_updates_cumulative + user_updates	-- Add up all values
		, LastBootupDateTime = @sql_server_boot_time				-- Reset LastBootupDateTime
	WHERE LastBootupDateTime < @sql_server_boot_time
	
-- The index exists AND @sql_server_boot_time	= t.LastBootupDateTime
UPDATE t
	SET [index_columns]				= s.[index_columns]			
		, included_columns			= s.included_columns			
		, filter					= s.filter					
		, index_type				= s.index_type				
		, is_primary_key			= s.is_primary_key			
		, is_unique					= s.is_unique					
		, is_disabled				= s.is_disabled				
		, row_count					= s.row_count					
		, reserved_MB				= s.reserved_MB				
		, size_MB					= s.size_MB					
		, fill_factor				= s.fill_factor				
		, user_seeks				= s.user_seeks		-- Always current data from DMVs
		, user_scans				= s.user_scans		-- Always current data from DMVs
		, user_lookups				= s.user_lookups	-- Always current data from DMVs		
		, user_updates				= s.user_updates	-- Always current data from DMVs
		, filegroup_desc			= s.filegroup_desc			
		, data_compression_desc		= s.data_compression_desc
        , LastDataCollectionTime    = GETDATE()
	FROM #result AS s
		INNER JOIN dbo.IndexUsageStats AS t
		ON s.dbname						= t.dbname			COLLATE DATABASE_DEFAULT		
			AND s.tableName				= t.tableName		COLLATE DATABASE_DEFAULT	
			AND s.partition_number		= t.partition_number	
			AND s.index_name			= t.index_name		COLLATE DATABASE_DEFAULT
			AND s.index_id				= t.index_id
			AND @sql_server_boot_time	= t.LastBootupDateTime
		
	DROP TABLE #result	
	DROP TABLE #databases	
