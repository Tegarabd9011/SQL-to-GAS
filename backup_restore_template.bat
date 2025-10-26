@echo off
REM === SQL Server Backup and Restore Template ===
REM Edit the variables below before running

set SRC_SERVER=localhost
set SRC_DB=YourSourceDB
set BACKUP_PATH=E:\Backup\YourSourceDB.bak

REM Backup the source database
echo Backing up %SRC_DB% on %SRC_SERVER% to %BACKUP_PATH%
sqlcmd -S %SRC_SERVER% -Q "BACKUP DATABASE [%SRC_DB%] TO DISK = N'%BACKUP_PATH%' WITH INIT, COPY_ONLY"

REM Restore to destination server
set DEST_SERVER=localhost\SQLEXPRESS
set DEST_DB=YourRestoredDB
set DEST_MDF=E:\SQLData\%DEST_DB%.mdf
set DEST_LDF=E:\SQLData\%DEST_DB%_log.ldf

echo Restoring to %DEST_DB% on %DEST_SERVER%
sqlcmd -S %DEST_SERVER% -Q "RESTORE DATABASE [%DEST_DB%] FROM DISK = N'%BACKUP_PATH%' WITH REPLACE, MOVE '%SRC_DB%' TO '%DEST_MDF%', MOVE '%SRC_DB%_log' TO '%DEST_LDF%'"

echo Done.
pause
