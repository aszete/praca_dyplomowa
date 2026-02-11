/*
===============================================================================
Stored Procedure: Load Bronze Layer from OLTP Source
===============================================================================
Purpose:
    Automates the ingestion of raw data from OLTP source extracts (CSV files) 
    into the Bronze layer. 

    Key Features:
        1. Dynamic Orchestration: Iterates through a defined list of tables 
           to minimize code duplication.
        2. Clean Ingestion: Truncates existing data before using BULK INSERT 
           for high-performance loading.
        3. Automated Auditing: Populates batch_id and load_date for every row
           and records execution metrics (start/end times, row counts, errors)
           into the bronze.metadata table.

    This procedure implements the initial ingestion step of the Medallion 
    Architecture:
        Source OLTP → Bronze (Raw) → Silver (Cleansed) → Gold (Dimensional)

Parameters:
    @batch_id VARCHAR(50) - Optional batch identifier.
                            If NULL, a unique ID is generated based on 
                            the current system timestamp.

Usage:
    -- Load all tables with auto-generated batch_id
    EXEC bronze.load_bronze;

    -- Load all tables with a specific batch_id
    EXEC bronze.load_bronze @batch_id = 'BATCH_2024_001';
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze
    @batch_id VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. Initialize Variables
    DECLARE @start_time DATETIME2;
    DECLARE @row_count INT;
    DECLARE @table_name VARCHAR(100);
    DECLARE @file_path NVARCHAR(500);
    DECLARE @sql_command NVARCHAR(MAX);

    IF @batch_id IS NULL
        SET @batch_id = 'BATCH_' + FORMAT(SYSDATETIME(), 'yyyyMMdd_HHmmss');

    -- 2. Define the list of tables to load
    -- You can easily add or remove tables here
    DECLARE @tables TABLE (name VARCHAR(100));
    INSERT INTO @tables (name)
    VALUES ('customers'), ('addresses'), ('brands'), ('categories'), 
           ('products'), ('order_item_returns'), ('order_items'), 
           ('orders'), ('pageviews'), ('payment_methods'), ('website_sessions');

    -- 3. Loop through each table
    DECLARE table_cursor CURSOR FOR SELECT name FROM @tables;
    OPEN table_cursor;
    FETCH NEXT FROM table_cursor INTO @table_name;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @start_time = SYSDATETIME();
        SET @file_path = 'C:\Projekt dyplomowy\bronze\' + @table_name + '.csv';

        BEGIN TRY
            PRINT '>> Loading: bronze.' + @table_name;

            -- Dynamic Truncate
            SET @sql_command = 'TRUNCATE TABLE bronze.' + QUOTENAME(@table_name);
            EXEC sp_executesql @sql_command;

            -- Dynamic Bulk Insert
            SET @sql_command = '
                BULK INSERT bronze.' + QUOTENAME(@table_name) + '
                FROM ''' + @file_path + '''
                WITH (
                    FIRSTROW = 2,
                    FIELDTERMINATOR = '','',
                    ROWTERMINATOR = ''0x0a'',
                    KEEPNULLS,
                    TABLOCK
                );';
            EXEC sp_executesql @sql_command;

            -- Dynamic Update for Batch Metadata
            SET @sql_command = '
                UPDATE bronze.' + QUOTENAME(@table_name) + '
                SET batch_id = @b, load_date = GETDATE()
                WHERE batch_id IS NULL;';
            EXEC sp_executesql @sql_command, N'@b VARCHAR(50)', @b = @batch_id;

            -- Get Row Count
            SET @sql_command = 'SELECT @rc = COUNT(*) FROM bronze.' + QUOTENAME(@table_name);
            EXEC sp_executesql @sql_command, N'@rc INT OUTPUT', @rc = @row_count OUTPUT;

            -- Log Success
            INSERT INTO bronze.metadata (table_name, load_start_time, load_end_time, last_load_date, batch_id, row_count, status)
            VALUES (@table_name, @start_time, SYSDATETIME(), SYSDATETIME(), @batch_id, @row_count, 'Success');

            PRINT '    Rows Loaded: ' + CAST(@row_count AS VARCHAR);
        END TRY
        BEGIN CATCH
            -- Log Failure
            INSERT INTO bronze.metadata (table_name, load_start_time, load_end_time, last_load_date, batch_id, status, error_message)
            VALUES (@table_name, @start_time, SYSDATETIME(), SYSDATETIME(), @batch_id, 'Failed', ERROR_MESSAGE());
            
            PRINT 'ERROR loading ' + @table_name + ': ' + ERROR_MESSAGE();
        END CATCH

        FETCH NEXT FROM table_cursor INTO @table_name;
    END

    CLOSE table_cursor;
    DEALLOCATE table_cursor;
END;
