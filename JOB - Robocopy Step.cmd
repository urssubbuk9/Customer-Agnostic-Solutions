@ECHO OFF
robocopy C:\temp2 x:\temp3 /MIR
ECHO %ERRORLEVEL%
if %ERRORLEVEL% LSS 8 EXIT 0
EXIT 1
REM Exit Values
REM 0	No files were copied. No failure was encountered. No files were mismatched. The files already exist in the destination directory; therefore, the copy operation was skipped.
REM 1	All files were copied successfully.
REM 2	There are some additional files in the destination directory that are not present in the source directory. No files were copied.
REM 3	Some files were copied. Additional files were present. No failure was encountered.
REM 5	Some files were copied. Some files were mismatched. No failure was encountered.
REM 6	Additional files and mismatched files exist. No files were copied and no failures were encountered. This means that the files already exist in the destination directory.
REM 7	Files were copied, a file mismatch was present, and additional files were present.
REM 8	Several files did not copy.
REM Any value greater than 8 indicates that there was at least one failure during the copy operation.
