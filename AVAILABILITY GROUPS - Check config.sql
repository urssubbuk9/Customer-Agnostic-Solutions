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
-- Description:	Check AG configuration for scenarios where there are 2 data centres
--					and the primary one should be SYNCHRONOUS_COMMIT with AUTOMATIC failover 
--					and the secondary have only one replica in SYNCHRONOUS_COMMIT and both in MANUAL failover
--
-- Assumptions:	This script needs to be run on the primary to generate any output
--				This script will only output the necessary changes to go back to the desired configuration
--
-- Log History:	22/01/2020 RAG	Created
--
-- =============================================

SET NOCOUNT ON

DECLARE @agname SYSNAME = 'AGNAME'

-- =============================================
-- DO NOT MODIFY BELOW THIS MESSAGE, 
--	UNLESS YOU KNOW WHAT YOU'RE DOING
-- =============================================

DECLARE @tsql NVARCHAR(MAX)	= N''

DECLARE   @pri_replica		SYSNAME
		, @pri_sync_mode	NVARCHAR(60)
		, @pri_role			NVARCHAR(60)
		, @pri_failover		NVARCHAR(60)
		, @pri_dc			NVARCHAR(60)

-- for the cursor
DECLARE @replica		SYSNAME
		, @sync_mode	NVARCHAR(60)
		, @role			NVARCHAR(60)
		, @failover		NVARCHAR(60)
		, @dc			NVARCHAR(60)
		, @n_replica	INT

-- CREATE Temp table with the replicas and data centre
IF OBJECT_ID('tempdb..#t') IS NOT NULL DROP TABLE #t

CREATE TABLE #t (replica_server_name SYSNAME, data_centre SYSNAME)

INSERT INTO #t (replica_server_name, data_centre)
VALUES
  ('SERVER1', 'DC1')
, ('SERVER2', 'DC1')
, ('SERVER3', 'DC2')
, ('SERVER4', 'DC3')

-- Create temp table to get AG replicas information
IF OBJECT_ID('tempdb..#agreplicas') IS NOT NULL DROP TABLE #agreplicas

CREATE TABLE #agreplicas(
	replica_server_name			SYSNAME
	, availability_mode_desc	NVARCHAR(60)
	, role_desc					NVARCHAR(60)
	, failover_mode_desc		NVARCHAR(60)
	, data_centre				NVARCHAR(60)
	, n_replica					INT
)

INSERT INTO #agreplicas
SELECT agr.replica_server_name 
		, agr.availability_mode_desc
		, agrs.role_desc
		, agr.failover_mode_desc
		, t.data_centre
		, ROW_NUMBER() OVER(PARTITION BY data_centre ORDER BY t.data_centre, agrs.role_desc) AS n_replica
	FROM sys.availability_groups AS ag
		INNER JOIN sys.availability_replicas AS agr
			ON agr.group_id = ag.group_id
		INNER JOIN sys.dm_hadr_availability_replica_states AS agrs
			ON agrs.group_id = ag.group_id
				AND agrs.replica_id = agr.replica_id
		INNER JOIN #t AS t
			ON t.replica_server_name = agr.replica_server_name 
	WHERE (@agname IS NULL OR ag.name LIKE @agname)

-- Replace the query above with this for testing purposes
--SELECT * FROM (values
--  ('SERVER1',	'SYNCHRONOUS_COMMIT'	, 'PRIMARY'		, 'AUTOMATIC'	, 'DC1', 1)
--, ('SERVER2',	'SYNCHRONOUS_COMMIT'	, 'SECONDARY'	, 'AUTOMATIC'	, 'DC1', 2)
--, ('SERVER3',	'SYNCHRONOUS_COMMIT'	, 'SECONDARY'	, 'MANUAL'		, 'DC2', 1)
--, ('SERVER4',	'ASYNCHRONOUS_COMMIT'	, 'SECONDARY'	, 'MANUAL'		, 'DC2', 2)
--) t(replica_server_name , availability_mode_desc, role_desc, failover_mode_desc, data_centre, n_replica)

-- Get the current PRIMARY	
SELECT @pri_replica			= replica_server_name 
		, @pri_sync_mode	= availability_mode_desc
		, @pri_role			= role_desc
		, @pri_failover		= failover_mode_desc
		, @pri_dc			= data_centre
	FROM #agreplicas
	WHERE role_desc = 'PRIMARY'

--SELECT @agname AS agname, @pri_replica AS pri_replica, @pri_sync_mode AS pri_sync_mode
--		, @pri_role AS pri_role, @pri_failover AS pri_failover, @pri_dc AS pri_dc		

IF @pri_replica IS NOT NULL  BEGIN
-- ONLY EXECUTE ON THE CURRENT PRIMARY REPLICA

	DECLARE c CURSOR LOCAL READ_ONLY FORWARD_ONLY FAST_FORWARD FOR
	SELECT replica_server_name 
			, availability_mode_desc
			, role_desc
			, failover_mode_desc
			, data_centre
			, n_replica
		FROM #agreplicas
		ORDER BY role_desc
				, data_centre
				, n_replica

	OPEN c
	FETCH NEXT FROM c INTO @replica, @sync_mode, @role, @failover, @dc, @n_replica

	WHILE @@FETCH_STATUS = 0 BEGIN

		IF @dc = @pri_dc BEGIN
			SET @tsql += '-- Primary data centre ' + @role + ' replica changes' + CHAR(10)
			
		    -- In the primary data centre both replicas to be SYNCHRONOUS_COMMIT and AUTOMATIC failover
			IF @sync_mode <> 'SYNCHRONOUS_COMMIT' BEGIN
				SET @tsql += 'ALTER AVAILABILITY GROUP ' + QUOTENAME(@agname) + ' MODIFY REPLICA ON N''' + @replica + ''' WITH (AVAILABILITY_MODE = SYNCHRONOUS_COMMIT)' + CHAR(10)
			END
			IF @failover <> 'AUTOMATIC' BEGIN
				SET @tsql += 'ALTER AVAILABILITY GROUP ' + QUOTENAME(@agname) + ' MODIFY REPLICA ON N''' + @replica + ''' WITH (FAILOVER_MODE = AUTOMATIC)' + CHAR(10)
			END
		END
		ELSE BEGIN
			SET @tsql += '-- Secondary data centre changes' + CHAR(10)
		
            -- Secondary data centre, ony one replica to be SYNCHRONOUS_COMMIT
			IF @n_replica = 1 BEGIN
			-- First replica in the PASSIVE data centre to be SYNCHRONOUS_COMMIT 
				IF @sync_mode <> 'SYNCHRONOUS_COMMIT' BEGIN
					SET @tsql += 'ALTER AVAILABILITY GROUP ' + QUOTENAME(@agname) + ' MODIFY REPLICA ON N''' + @replica + ''' WITH (AVAILABILITY_MODE = SYNCHRONOUS_COMMIT)' + CHAR(10)
				END
			END
			ELSE BEGIN
				IF @sync_mode <> 'ASYNCHRONOUS_COMMIT' BEGIN
					SET @tsql += 'ALTER AVAILABILITY GROUP ' + QUOTENAME(@agname) + ' MODIFY REPLICA ON N''' + @replica + ''' WITH (AVAILABILITY_MODE = ASYNCHRONOUS_COMMIT)' + CHAR(10)
				END
			END
			
			-- All replicas in Secondary data centre to be MANUAL failover
			IF @failover <> 'MANUAL' BEGIN
				SET @tsql += 'ALTER AVAILABILITY GROUP ' + QUOTENAME(@agname) + ' MODIFY REPLICA ON N''' + @replica + ''' WITH (FAILOVER_MODE = MANUAL)' + CHAR(10)
			END
		END
		
		FETCH NEXT FROM c INTO @replica, @sync_mode, @role, @failover, @dc, @n_replica
	END

	CLOSE c
	DEALLOCATE c

	IF @tsql <> '' BEGIN
		PRINT @tsql
	END
	ELSE BEGIN
		PRINT 'All replicas are configured as expected which is
	- PRIMARY DATA CENTRE (currently ' + @pri_dc + '), both replicas on SYNCHRONOUS_COMMIT and AUTOMATIC failover
	- SECONDARY DATA CENTER, one replica on SYNCHRONOUS_COMMIT and the other ASYNCHRONOUS_COMMIT, both replicas with MANUAL failover'
		SELECT replica_server_name
				, data_centre
				, role_desc
				, availability_mode_desc
				, failover_mode_desc
			FROM #agreplicas
			ORDER BY role_desc, data_centre, n_replica
	END
END
ELSE BEGIN
	PRINT 'The server ' + QUOTENAME(@@SERVERNAME) + ' is not the primary replica for the AG ' + QUOTENAME(@agname)
END
