-- Find spids for currently running jobs steps which run Ola's IndexOptimize procedure
declare @indexingSpids table (
	session_id int
)
declare @indexingSpids1 table (
	session_id int,
	blocking_session_id int
)
declare @indexingSpids2 table (
	session_id int,
	blocking_session_id int

)
-- output to job history the current spid, so that any kills logged in the error log can be connected with this job
print 'Monitor (this job) SPID: ' + convert(nvarchar(5), @@SPID)

-- Only list IndexOptimize spids which are blocked
insert into @indexingSpids1 (session_id, blocking_session_id)
select er.session_id,
	er.blocking_session_id
from msdb.dbo.sysjobsteps js
	inner join sys.dm_exec_sessions es on es.program_name = 'SQLAgent - TSQL JobStep (Job ' + upper(master.sys.fn_varbintohexstr(js.job_id)) + ' : Step ' + convert(nvarchar(3), js.step_id) + ')'
	inner join sys.dm_exec_requests er on es.session_id = er.session_id
where js.command like '%indexoptimize%'
	and er.blocking_session_id <> 0

if exists (select top 1 1 from @indexingSpids1)
	begin
	-- blocked IndexOptimize processes found, see if they are still blocked after a short period
	-- To avoid killing spids when the blocking is brief, we will wait instead
	waitfor delay '00:01:00'

	insert into @indexingSpids2 (session_id, blocking_session_id)
	select er.session_id,
		er.blocking_session_id
	from msdb.dbo.sysjobsteps js
		inner join sys.dm_exec_sessions es on es.program_name = 'SQLAgent - TSQL JobStep (Job ' + upper(master.sys.fn_varbintohexstr(js.job_id)) + ' : Step ' + convert(nvarchar(3), js.step_id) + ')'
		inner join sys.dm_exec_requests er on es.session_id = er.session_id
	where js.command like '%indexoptimize%'
		and er.blocking_session_id <> 0

	-- Only kill spids which are blocking IndexOptimize jobs where the blocking spid has not changed
	insert into @indexingSpids (session_id)
	select first.session_id
	from @indexingSpids1 first
		inner join @indexingSpids2 second on first.session_id = second.session_id
			and first.blocking_session_id = second.blocking_session_id
	end

declare @spid int,
	@cmd nvarchar(100),
	@program_name nvarchar(100),
	@emailbody nvarchar(400)

set @emailbody = ''

declare killcsr cursor local fast_forward
for
	WITH blocking (session_id, blocking_session_id)
	AS (
		SELECT r.session_id,
			r.blocking_session_id
		FROM SYS.DM_EXEC_REQUESTS r
			INNER JOIN @indexingSpids s on r.session_id = s.session_id
		WHERE r.blocking_session_id <> 0

		UNION ALL

		SELECT er.session_id,
			er.blocking_session_id
		FROM SYS.DM_EXEC_REQUESTS er
			INNER JOIN blocking b on er.session_id = b.blocking_session_id
	)
	-- Return only the head blocker of the chain
	SELECT b.session_id,
		es.program_name
	from (
		SELECT session_id
		FROM blocking
		where blocking_session_id = 0
		union
		select blocking_session_id
		from blocking
		where blocking_session_id not in (select session_id from sys.dm_exec_requests)
	) b
		inner join sys.dm_exec_sessions es on es.session_id = b.session_id

OPEN killcsr
FETCH NEXT FROM killcsr INTO @spid, @program_name
WHILE @@FETCH_STATUS = 0
	BEGIN
	IF @program_name = 'SQL Anywhere'
		BEGIN
		set @cmd = 'kill ' + convert(nvarchar(5), @spid)
		print @cmd
		set @emailbody = @emailbody + convert(nvarchar, getdate(),25) + ' Killing SQL Anywhere SPID ' + convert(nvarchar(5), @spid) + CHAR(10) + CHAR(13)
		exec sp_executesql @cmd
		END
	ELSE
		BEGIN
		IF @program_name LIKE 'SQLAgent - TSQL JobStep%'
		BEGIN
			DECLARE @job_id varchar(34) = SUBSTRING(@program_name,CHARINDEX('(Job ',@program_name)+5,34) 
			SELECT @program_name = 'Job: ' + name from msdb.dbo.sysjobs where UPPER(master.dbo.fn_varbintohexstr(job_id)) = @job_id
		END
		SET @emailbody = @emailbody + convert(nvarchar, getdate(),25) + ' Not killing ' + @program_name + ', SPID ' + convert(nvarchar(5), @spid) + CHAR(10) + CHAR(13)
		END
	FETCH NEXT FROM killcsr INTO @spid, @program_name
	END
CLOSE killcsr
DEALLOCATE killcsr

-- If any work done, send an email
if @emailbody <> ''
	begin
	set @emailbody = 'Blocking of the index maintenance job(s) on <ServerName> detected.  The following actions were taken' + CHAR(10) + CHAR(13) + @emailbody
	EXEC msdb.dbo.sp_send_dbmail
		@recipients = 'dba@coeo.com',
		@subject = 'Index Maintenance is Blocked',
		@body =  @emailbody,
		@body_format = 'TEXT',
		@from_address = 'noreply@coeo.com'
	end
