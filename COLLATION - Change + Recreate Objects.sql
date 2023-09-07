/*
===============================================================================
RUN AGAINST TARGET DB

NEED TO SET THE DESIRED COLLATION FOR THE DB

THIS SCRIPT DETECTS COLUMNS NOT USING THIS COLLATION AND GENERATES DROP AND CREATE SCRIPTS FOR OBJECTS DEPENDING ON THESE COLUMNS

DEPENDENCIES ARE THEN DROPPED, COLLATION IS CHANGED, THE DEPENDENCIES ARE THEN RECREATED

THIS IS SAFE TO RUN, THIS SCRIPT ONLY GENERATES T-SQL

SW - 09/2022
===============================================================================
*/


/*
===============================================================================
SET PARAMS
===============================================================================
*/

DECLARE @collationTarget NVARCHAR(255) = 'Latin1_General_CI_AS'

/*
===============================================================================
*/

--tempdb tables drop if exists (for reruns)
IF OBJECT_ID('tempdb..#tablesColumnsNotUsingTargetCollation') IS NOT NULL BEGIN DROP TABLE #tablesColumnsNotUsingTargetCollation END
IF OBJECT_ID('tempdb..#checkedForeignkeys') IS NOT NULL BEGIN DROP TABLE #checkedForeignkeys END
IF OBJECT_ID('tempdb..#checkedPKConstraints') IS NOT NULL BEGIN DROP TABLE #checkedPKConstraints END
IF OBJECT_ID('tempdb..#checkedConstraints') IS NOT NULL BEGIN DROP TABLE #checkedConstraints END
IF OBJECT_ID('tempdb..#checkedIndexes') IS NOT NULL BEGIN DROP TABLE #checkedIndexes END
IF OBJECT_ID('tempdb..#foreignkeys') IS NOT NULL BEGIN DROP TABLE #foreignkeys END
IF OBJECT_ID('tempdb..#pKConstraints') IS NOT NULL BEGIN DROP TABLE #pKConstraints END
IF OBJECT_ID('tempdb..#indexes') IS NOT NULL BEGIN DROP TABLE #indexes END
IF OBJECT_ID('tempdb..#statistics') IS NOT NULL BEGIN DROP TABLE #statistics END

/*
======== Tables/Columns Not Using Target Collation =========
*/

SELECT t.name TableName, c.name ColumnName, collation_name, c.is_ansi_padded, c.is_nullable, c.is_computed, c.is_replicated
INTO #tablesColumnsNotUsingTargetCollation
FROM sys.columns c
inner join sys.tables t on c.object_id = t.object_id
where collation_name <> @collationTarget


/*
======== CHECK COMPUTED COLUMNS - NO AUTO DROP/CREATE  ========
*/

IF EXISTS (
SELECT 1
  FROM sys.computed_columns cc INNER JOIN
       sys.sql_dependencies sd ON cc.object_id=sd.object_id AND
                                  cc.column_id=sd.column_id AND
                                  sd.object_id=sd.referenced_major_id INNER JOIN
       sys.columns c ON c.object_id=sd.referenced_major_id AND
                        c.column_id = sd.referenced_minor_id
   WHERE c.collation_name IS NOT NULL AND c.collation_name <> @collationTarget
   AND sd.class=1)

SELECT 
'WARNING! DROP THESE OBJECTS MANUALLY AND RECREATE AFTER',
OBJECT_NAME(c.object_id) "Table Name",
       COL_NAME(sd.referenced_major_id, sd.referenced_minor_id) "Column Name",
       c.collation_name "Collation",
       definition "Definition"
  FROM sys.computed_columns cc INNER JOIN
       sys.sql_dependencies sd ON cc.object_id=sd.object_id AND
                                  cc.column_id=sd.column_id AND
                                  sd.object_id=sd.referenced_major_id INNER JOIN
       sys.columns c ON c.object_id=sd.referenced_major_id AND
                        c.column_id = sd.referenced_minor_id
   WHERE c.collation_name IS NOT NULL AND c.collation_name <> @collationTarget
   AND sd.class=1

/*
======== CHECK FKs ========
*/

IF EXISTS (
SELECT 1
     FROM sys.foreign_keys AS f INNER JOIN
          sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id INNER JOIN
          sys.columns c1 ON c1.object_id=fc.parent_object_id AND
                            c1.column_id=fc.parent_column_id INNER JOIN
          sys.columns c2 ON c2.object_id=fc.parent_object_id AND
                            c2.column_id=fc.parent_column_id
    WHERE (c1.collation_name IS NOT NULL AND c1.collation_name <> @collationTarget)
       OR (c2.collation_name IS NOT NULL AND c2.collation_name <> @collationTarget))

   SELECT f.name "Foreign Key Name",
          OBJECT_NAME(f.parent_object_id) "Table Name",
		      OBJECT_SCHEMA_NAME(f.parent_object_id) "Schema",
          COL_NAME(fc.parent_object_id,fc.parent_column_id) "Column Name",
          c1.collation_name "Column 1 Collation",
          OBJECT_NAME (f.referenced_object_id) "Reference Table Name",
          COL_NAME(fc.referenced_object_id,fc.referenced_column_id) "Reference Column Name",
          c2.collation_name "Column 2 Collation"
     INTO #checkedForeignKeys
     FROM sys.foreign_keys AS f INNER JOIN
          sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id INNER JOIN
          sys.columns c1 ON c1.object_id=fc.parent_object_id AND
                            c1.column_id=fc.parent_column_id INNER JOIN
          sys.columns c2 ON c2.object_id=fc.parent_object_id AND
                            c2.column_id=fc.parent_column_id
    WHERE (c1.collation_name IS NOT NULL AND c1.collation_name <> @collationTarget)
       OR (c2.collation_name IS NOT NULL AND c2.collation_name <> @collationTarget)

/*
======== CHECK PKs ========
*/

   IF EXISTS (
   SELECT 1
     FROM sys.indexes AS i INNER JOIN
          sys.index_columns AS ic ON i.object_id = ic.object_id AND
                                     i.index_id = ic.index_id INNER JOIN
          sys.columns c ON ic.object_id=c.object_id AND
                           c.column_id=ic.column_id
    WHERE i.is_primary_key=1
      AND c.collation_name IS NOT NULL AND c.collation_name <> @collationTarget
	  )
   SELECT i.name AS "Primary Key Name",
          OBJECT_NAME(ic.object_id) "Table Name",
          COL_NAME(ic.object_id,ic.column_id) "Column Name",
          c.collation_name "Collation"
     INTO #checkedPKConstraints
     FROM sys.indexes AS i INNER JOIN
          sys.index_columns AS ic ON i.object_id = ic.object_id AND
                                     i.index_id = ic.index_id INNER JOIN
          sys.columns c ON ic.object_id=c.object_id AND
                           c.column_id=ic.column_id
    WHERE i.is_primary_key=1
      AND c.collation_name IS NOT NULL AND c.collation_name <> @collationTarget

/*
======== CHECK INDEXES ========
*/

	IF EXISTS (
	SELECT 1
        FROM sys.indexes AS i INNER JOIN
             sys.index_columns AS ic ON i.object_id = ic.object_id AND
                                        i.index_id = ic.index_id INNER JOIN
             sys.columns c ON ic.object_id=c.object_id AND
                              c.column_id=ic.column_id
       WHERE c.collation_name IS NOT NULL AND c.collation_name <> @collationTarget
         AND i.is_primary_key <> 1
         AND OBJECT_NAME(ic.object_id) NOT LIKE 'sys%'
		 )
      SELECT 
	  i.name AS "Index Name",
            ic.object_id,
             OBJECT_NAME(ic.object_id) "Table Name",
             COL_NAME(ic.object_id,ic.column_id) "Column Name",
             c.collation_name "Collation"
        INTO #checkedIndexes
        FROM sys.indexes AS i INNER JOIN
             sys.index_columns AS ic ON i.object_id = ic.object_id AND
                                        i.index_id = ic.index_id INNER JOIN
             sys.columns c ON ic.object_id=c.object_id AND
                              c.column_id=ic.column_id
       WHERE c.collation_name IS NOT NULL AND c.collation_name <> @collationTarget
         AND i.is_primary_key <> 1
         AND OBJECT_NAME(ic.object_id) NOT LIKE 'sys%'
         AND OBJECT_NAME(ic.object_id) NOT LIKE 'sqlagent%'


/*
======== CHECK CONSTRAINTS ========
*/

IF EXISTS (
SELECT 1
            FROM sys.check_constraints cc INNER JOIN
                 sys.sql_dependencies sd ON cc.object_id=sd.object_id INNER JOIN
                 sys.columns c ON c.object_id=sd.referenced_major_id AND
                                  c.column_id = sd.referenced_minor_id
           WHERE c.collation_name IS NOT NULL AND c.collation_name <> @collationTarget
             AND cc.type = 'C'
             AND sd.class=1
			 )
          SELECT OBJECT_NAME(cc.object_id) "Constraint Name",
                 OBJECT_Name(c.object_id) "Table Name",
                 COL_NAME(sd.referenced_major_id, sd.referenced_minor_id) "Column Name",
                 c.collation_name "Collation",
                 definition "Definition"
            INTO #checkedConstraints
            FROM sys.check_constraints cc INNER JOIN
                 sys.sql_dependencies sd ON cc.object_id=sd.object_id INNER JOIN
                 sys.columns c ON c.object_id=sd.referenced_major_id AND
                                  c.column_id = sd.referenced_minor_id
           WHERE c.collation_name IS NOT NULL AND c.collation_name <> @collationTarget
             AND cc.type = 'C'
             AND sd.class=1


/*
======== DROP / CREATE FK CONSTRAINTS ========
*/

IF OBJECT_ID('tempdb..#checkedForeignkeys') IS NOT NULL BEGIN

CREATE TABLE #foreignkeys 
(
  drop_script NVARCHAR(MAX),
  create_script NVARCHAR(MAX)
);
  
DECLARE @drop   NVARCHAR(MAX) = N'',
        @create NVARCHAR(MAX) = N'';


SELECT @drop += N'
ALTER TABLE ' + QUOTENAME(cs.name) + '.' + QUOTENAME(ct.name) 
    + ' DROP CONSTRAINT ' + QUOTENAME(fk.name) + ';'
FROM sys.foreign_keys AS fk
INNER JOIN sys.tables AS ct
  ON fk.parent_object_id = ct.[object_id]
INNER JOIN sys.schemas AS cs 
  ON ct.[schema_id] = cs.[schema_id]
WHERE fk.name IN
(
  SELECT [Foreign Key Name] FROM #checkedForeignKeys
)

INSERT #foreignkeys(drop_script) SELECT @drop;

SELECT @create += CHAR(13) + CHAR(10) + N'
ALTER TABLE ' 
   + QUOTENAME(cs.name) + '.' + QUOTENAME(ct.name) 
   + ' ADD CONSTRAINT ' + QUOTENAME(fk.name) 
   + ' FOREIGN KEY (' + STUFF((SELECT ',' + QUOTENAME(c.name)
   -- get all the columns in the constraint table
    FROM sys.columns AS c 
    INNER JOIN sys.foreign_key_columns AS fkc 
    ON fkc.parent_column_id = c.column_id
    AND fkc.parent_object_id = c.[object_id]
    WHERE fkc.constraint_object_id = fk.[object_id]
    ORDER BY fkc.constraint_column_id 
    FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 1, N'')
  + ') REFERENCES ' + QUOTENAME(rs.name) + '.' + QUOTENAME(rt.name)
  + '(' + STUFF((SELECT ',' + QUOTENAME(c.name)
   -- get all the referenced columns
    FROM sys.columns AS c 
    INNER JOIN sys.foreign_key_columns AS fkc 
    ON fkc.referenced_column_id = c.column_id
    AND fkc.referenced_object_id = c.[object_id]
    WHERE fkc.constraint_object_id = fk.[object_id]
    ORDER BY fkc.constraint_column_id 
    FOR XML PATH(N''), TYPE).value(N'.[1]', N'nvarchar(max)'), 1, 1, N'') + ');' + CHAR(13) + CHAR(10)
FROM sys.foreign_keys AS fk
INNER JOIN sys.tables AS rt -- referenced table
  ON fk.referenced_object_id = rt.[object_id]
INNER JOIN sys.schemas AS rs 
  ON rt.[schema_id] = rs.[schema_id]
INNER JOIN sys.tables AS ct -- constraint table
  ON fk.parent_object_id = ct.[object_id]
INNER JOIN sys.schemas AS cs 
  ON ct.[schema_id] = cs.[schema_id]
WHERE rt.is_ms_shipped = 0 AND ct.is_ms_shipped = 0
and fk.name IN
(
  SELECT [Foreign Key Name] FROM #checkedForeignKeys
)

UPDATE #foreignkeys SET create_script = @create;

END

/*
======== DROP / CREATE PKS & CONSTRAINTS ========
*/

--drop primary keys and unique constraints - variables - START
declare @SchemaName varchar(100)
declare @TableName varchar(256)
declare @IndexName varchar(256)
declare @ColumnName varchar(100)
declare @is_unique_constraint varchar(100)
declare @IndexTypeDesc varchar(100)
declare @FileGroupName varchar(100)
declare @is_disabled varchar(100)
declare @IndexOptions varchar(max)
declare @IndexColumnId int
declare @IsDescendingKey int 
declare @IsIncludedColumn int
declare @TSQLCreatePKConstraints varchar(max)
DECLARE @TSQLDropPKConstraints VARCHAR(MAX)
declare @is_primary_key varchar(100)
--drop primary keys and unique constraints - variables - END

IF OBJECT_ID('tempdb..#checkedPKConstraints') IS NOT NULL BEGIN

IF EXISTS (
 SELECT 1 from sys.tables tb 
 inner join sys.indexes ix on tb.object_id=ix.object_id
 inner join sys.index_columns ixc on ix.object_id=ixc.object_id and ix.index_id= ixc.index_id
 inner join sys.columns col on ixc.object_id =col.object_id  and ixc.column_id=col.column_id
 where ix.type>0 and ix.is_primary_key=1
 AND is_nullable = 1
 and 
 ix.name in 
(SELECT [Primary Key Name] FROM #checkedPKConstraints)
 )
 SELECT 'WARNING PRIMARY KEY CONSTRAINTS ON NULLABLE COLUMNS DETECTED, PLEASE RESOLVE BEFORE RUNNING SCRIPT'

CREATE TABLE #pKConstraints 
(
  drop_script NVARCHAR(MAX),
  create_script NVARCHAR(MAX)
);

declare CursorIndex cursor for
 select schema_name(t.schema_id) [schema_name], t.name, ix.name,
 case when ix.is_unique_constraint = 1 then ' UNIQUE ' else '' END 
    ,case when ix.is_primary_key = 1 then ' PRIMARY KEY ' else '' END 
 , ix.type_desc,
  case when ix.is_padded=1 then 'PAD_INDEX = ON, ' else 'PAD_INDEX = OFF, ' end
 + case when ix.allow_page_locks=1 then 'ALLOW_PAGE_LOCKS = ON, ' else 'ALLOW_PAGE_LOCKS = OFF, ' end
 + case when ix.allow_row_locks=1 then  'ALLOW_ROW_LOCKS = ON, ' else 'ALLOW_ROW_LOCKS = OFF, ' end
 + case when INDEXPROPERTY(t.object_id, ix.name, 'IsStatistics') = 1 then 'STATISTICS_NORECOMPUTE = ON, ' else 'STATISTICS_NORECOMPUTE = OFF, ' end
 + case when ix.ignore_dup_key=1 then 'IGNORE_DUP_KEY = ON' else 'IGNORE_DUP_KEY = OFF' end
 + case when ix.fill_factor <> 0 then ', FILLFACTOR =' + CAST(ix.fill_factor AS VARCHAR(3)) else '' end AS IndexOptions
 , FILEGROUP_NAME(ix.data_space_id) FileGroupName
 from sys.tables t 
 inner join sys.indexes ix on t.object_id=ix.object_id
 where ix.type>0 and  (ix.is_primary_key=1 or ix.is_unique_constraint=1) --and schema_name(tb.schema_id)= @SchemaName and tb.name=@TableName
 and t.is_ms_shipped=0 and t.name<>'sysdiagrams'
 and 
 ix.name in 
(SELECT [Primary Key Name] FROM #checkedPKConstraints)
 /*
 (SELECT t.name
FROM sys.columns c
inner join sys.tables t on c.object_id = t.object_id
where collation_name <> @collationTarget)
*/
 order by schema_name(t.schema_id), t.name, ix.name
open CursorIndex
fetch next from CursorIndex into  @SchemaName, @TableName, @IndexName, @is_unique_constraint, @is_primary_key, @IndexTypeDesc, @IndexOptions, @FileGroupName
while (@@fetch_status=0)
begin
 declare @IndexColumns varchar(max)
 declare @IncludedColumns varchar(max)
 set @IndexColumns=''
 set @IncludedColumns=''
 declare CursorIndexColumn cursor for 
 select col.name, ixc.is_descending_key, ixc.is_included_column
 from sys.tables tb 
 inner join sys.indexes ix on tb.object_id=ix.object_id
 inner join sys.index_columns ixc on ix.object_id=ixc.object_id and ix.index_id= ixc.index_id
 inner join sys.columns col on ixc.object_id =col.object_id  and ixc.column_id=col.column_id
 where ix.type>0 and (ix.is_primary_key=1 or ix.is_unique_constraint=1)
 and schema_name(tb.schema_id)=@SchemaName and tb.name=@TableName and ix.name=@IndexName
 order by ixc.key_ordinal
 open CursorIndexColumn 
 fetch next from CursorIndexColumn into  @ColumnName, @IsDescendingKey, @IsIncludedColumn
 while (@@fetch_status=0)
 begin
  if @IsIncludedColumn=0 
    set @IndexColumns=@IndexColumns + @ColumnName  + case when @IsDescendingKey=1  then ' DESC, ' else  ' ASC, ' end
  else 
   set @IncludedColumns=@IncludedColumns  + @ColumnName  +', ' 
     
  fetch next from CursorIndexColumn into @ColumnName, @IsDescendingKey, @IsIncludedColumn
 end
 close CursorIndexColumn
 deallocate CursorIndexColumn
 set @IndexColumns = substring(@IndexColumns, 1, len(@IndexColumns)-1)
 set @IncludedColumns = case when len(@IncludedColumns) >0 then substring(@IncludedColumns, 1, len(@IncludedColumns)-1) else '' end
--  print @IndexColumns
--  print @IncludedColumns

set @TSQLCreatePKConstraints =''
set @TSQLDropPKConstraints =''
set  @TSQLCreatePKConstraints=
'ALTER TABLE '+  QUOTENAME(@SchemaName) +'.'+ QUOTENAME(@TableName)+ ' ADD CONSTRAINT ' +  QUOTENAME(@IndexName) + @is_unique_constraint + @is_primary_key + +@IndexTypeDesc +  '('+@IndexColumns+') '+ 
 case when len(@IncludedColumns)>0 then CHAR(13) +'INCLUDE (' + @IncludedColumns+ ')' else '' end + CHAR(13)+'WITH (' + @IndexOptions+ ') ON ' + QUOTENAME(@FileGroupName) + ';'  

SET @TSQLDropPKConstraints = 'ALTER TABLE '+QUOTENAME(@SchemaName)+ '.' + QUOTENAME(@TableName) + ' DROP CONSTRAINT ' +QUOTENAME(@IndexName)

INSERT INTO #PKConstraints (drop_script, create_script) 
SELECT @TSQLDropPKConstraints, @TSQLCreatePKConstraints

fetch next from CursorIndex into  @SchemaName, @TableName, @IndexName, @is_unique_constraint, @is_primary_key, @IndexTypeDesc, @IndexOptions, @FileGroupName

end
close CursorIndex
deallocate CursorIndex

END

/*
======== DROP / CREATE INDEXES ========
*/

IF OBJECT_ID('tempdb..#checkedIndexes') IS NOT NULL BEGIN

Select A.[object_id]
 , OBJECT_NAME(A.[object_id]) AS Table_Name
 , A.Index_ID
 , A.[Name] As Index_Name
 , CAST(
 Case When A.type = 1 AND is_unique = 1 Then 'CREATE UNIQUE CLUSTERED INDEX '
 When A.type = 1 AND is_unique = 0 Then 'CREATE CLUSTERED INDEX '
 When A.type = 2 AND is_unique = 1 Then 'CREATE UNIQUE NONCLUSTERED INDEX '
 When A.type = 2 AND is_unique = 0 Then 'CREATE NONCLUSTERED INDEX '
 End
 + quotename(A.[Name]) + ' ON ' + quotename(S.name) + '.' + quotename(OBJECT_NAME(A.[object_id])) + ' ('
 + Stuff(
 (
 Select
 ',[' + COL_NAME(A.[object_id],C.column_id)
 + Case When C.is_descending_key = 1 Then '] DESC' Else '] ASC' End
 From sys.index_columns C WITH (NOLOCK)
 Where A.[Object_ID] = C.object_id
 And A.Index_ID = C.Index_ID
 And C.is_included_column = 0
 Order by C.key_Ordinal Asc
 For XML Path('')
 )
 ,1,1,'') + ') '
 
 + CASE WHEN A.type = 1 THEN ''
 ELSE Coalesce('INCLUDE ('
 + Stuff(
 (
 Select
 ',' + QuoteName(COL_NAME(A.[object_id],C.column_id))
 From sys.index_columns C WITH (NOLOCK)
 Where A.[Object_ID] = C.object_id
 And A.Index_ID = C.Index_ID
 And C.is_included_column = 1
 Order by C.index_column_id Asc
 For XML Path('')
 )
 ,1,1,'') + ') '
 ,'') End
 + Case When A.has_filter = 1 Then 'Where ' + A.filter_definition Else '' End
 + ' WITH (SORT_IN_TEMPDB = ON'
 --DROP_EXISTING not including as we are dropping the index prior to this
 --SORT_IN_TEMPDB = ON is recommended but based on your own environment.
 + ', Fillfactor = ' + Cast(Case When fill_factor = 0 Then 100 Else fill_factor End As varchar(3)) 
 + Case When A.[is_padded] = 1 Then ', PAD_INDEX = ON' Else ', PAD_INDEX = OFF' END
 + Case When D.[no_recompute] = 1 Then ', STATISTICS_NORECOMPUTE = ON' Else ', STATISTICS_NORECOMPUTE = OFF' End 
 + Case When A.[ignore_dup_key] = 1 Then ', IGNORE_DUP_KEY = ON' Else ', IGNORE_DUP_KEY = OFF' End
 + Case When A.[ALLOW_ROW_LOCKS] = 1 Then ', ALLOW_ROW_LOCKS = ON' Else ', ALLOW_ROW_LOCKS = OFF' END
 + Case When A.[ALLOW_PAGE_LOCKS] = 1 Then ', ALLOW_PAGE_LOCKS = ON' Else ', ALLOW_PAGE_LOCKS = OFF' End 
 + Case When P.[data_compression] = 0 Then ', DATA_COMPRESSION = NONE' 
 When P.[data_compression] = 1 Then ', DATA_COMPRESSION = ROW' 
 Else ', DATA_COMPRESSION = PAGE' End 
 + ') On ' 
 + Case when C.type = 'FG' THEN quotename(C.name) 
 ELSE quotename(C.name) + '(' + F.Partition_Column + ')' END + ';' --if it uses partition scheme then need partition column
 As nvarchar(Max)) As create_script
 , C.name AS FileGroupName
 , 'DROP INDEX ' + quotename(A.[Name]) + ' On ' + quotename(S.name) + '.' + quotename(OBJECT_NAME(A.[object_id])) + ';' AS drop_script
 INTO #indexes
From SYS.Indexes A WITH (NOLOCK)
 INNER JOIN
 sys.objects B WITH (NOLOCK)
 ON A.object_id = B.object_id
 INNER JOIN 
 SYS.schemas S
 ON B.schema_id = S.schema_id 
 INNER JOIN
 SYS.data_spaces C WITH (NOLOCK)
 ON A.data_space_id = C.data_space_id 
 INNER JOIN
 SYS.stats D WITH (NOLOCK)
 ON A.object_id = D.object_id
 AND A.index_id = D.stats_id 
 Inner Join
 (
 select object_id, index_id, Data_Compression, ROW_NUMBER() Over(Partition By object_id, index_id Order by COUNT(*) Desc) As Main_Compression
 From sys.partitions WITH (NOLOCK)
 Group BY object_id, index_id, Data_Compression
 ) P
 ON A.object_id = P.object_id
 AND A.index_id = P.index_id
 AND P.Main_Compression = 1
 Outer APPLY
 (
 SELECT COL_NAME(A.object_id, E.column_id) AS Partition_Column
 From sys.index_columns E WITH (NOLOCK)
 WHERE E.object_id = A.object_id
 AND E.index_id = A.index_id
 AND E.partition_ordinal = 1
 ) F 
Where A.type IN (1,2) --clustered and nonclustered
 AND B.Type != 'S'
 AND OBJECT_NAME(A.[object_id]) not like 'queue_messages_%'
 AND OBJECT_NAME(A.[object_id]) not like 'filestream_tombstone_%'
 AND OBJECT_NAME(A.[object_id]) NOT LIKE 'sys%'
 AND OBJECT_NAME(A.[object_id]) NOT LIKE 'sqlagent%'
 AND A.is_primary_key = 0
 AND A.[object_id] IN (
 SELECT object_id FROM #checkedIndexes
 )

END

/*
==========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
OUTPUT // FOUND OBJECTS
==========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
*/

SELECT '--OBJECTS NOT USING TARGET COLLATION:'
IF OBJECT_ID('tempdb..#tablesColumnsNotUsingTargetCollation') IS NOT NULL BEGIN (SELECT 'TABLE/COLUMNS', * FROM #tablesColumnsNotUsingTargetCollation) END
IF OBJECT_ID('tempdb..#checkedForeignKeys') IS NOT NULL BEGIN (SELECT 'FOREIGN KEYS', * FROM #checkedForeignKeys) END
IF OBJECT_ID('tempdb..#checkedPKConstraints') IS NOT NULL BEGIN (SELECT 'PRIMARY KEYS', * FROM #checkedPKConstraints) END
IF OBJECT_ID('tempdb..#checkedIndexes') IS NOT NULL BEGIN (SELECT 'INDEXES', * FROM #checkedIndexes) END
IF OBJECT_ID('tempdb..#checkedConstraints') IS NOT NULL BEGIN (SELECT 'CONSTRAINTS', * FROM #checkedConstraints) END

/*
==========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
T-SQL TO RUN/SAVE
==========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
*/

/*DROP DEPENDENCES*/

SELECT '--DEPENDENCIES TO DROP:'
IF OBJECT_ID('tempdb..#checkedForeignKeys') IS NOT NULL BEGIN (SELECT drop_script "--DROP DEPENDENCIES - FOREIGN KEYS" FROM #foreignkeys) END
IF OBJECT_ID('tempdb..#pKConstraints') IS NOT NULL BEGIN (SELECT drop_script "--DROP DEPENDENCIES - PRIMARY KEYS / CONSTRAINTS" FROM #pKConstraints) END
IF OBJECT_ID('tempdb..#indexes') IS NOT NULL BEGIN (SELECT drop_script "--DROP DEPENDENCIES - INDEXES" FROM #indexes) END

/*ALTER COLLATION*/

SELECT 
--t.name TableName, c.name ColumnName, sc.DATA_TYPE, c.collation_name,
'ALTER TABLE ' + CAST(SCHEMA_NAME(t.schema_id) AS NVARCHAR(255)) + '.' + t.name + ' ALTER COLUMN ' 
+ '[' + c.name + ']'+ ' ' + sc.DATA_TYPE 
+ '(' + 

CASE WHEN sc.CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX'
ELSE 
CAST(sc.CHARACTER_MAXIMUM_LENGTH AS NVARCHAR(5))
END

  + ')' + ' COLLATE ' + @collationTarget + case when c.is_nullable = 0 then ' NOT NULL' else ' NULL' end

AS [ChangeCollation]
FROM sys.columns c
inner join sys.tables t on c.object_id = t.object_id
inner join INFORMATION_SCHEMA.COLUMNS sc on sc.TABLE_NAME = t.name and sc.COLUMN_NAME = c.name
where c.collation_name IS NOT NULL AND c.collation_name <> @collationTarget

/* RECREATE DEPENDENCIES */

SELECT '--DEPENDENCIES TO RECREATE:'
IF OBJECT_ID('tempdb..#pKConstraints') IS NOT NULL BEGIN (SELECT create_script "--CREATE DEPENDENCIES - PRIMARY KEYS" FROM #pKConstraints) END
IF OBJECT_ID('tempdb..#checkedForeignKeys') IS NOT NULL BEGIN (SELECT create_script "--CREATE DEPENDENCIES - FOREIGN KEYS" FROM #foreignkeys) END
IF OBJECT_ID('tempdb..#indexes') IS NOT NULL BEGIN (SELECT create_script "--CREATE DEPENDENCIES - INDEXES" FROM #indexes) END
