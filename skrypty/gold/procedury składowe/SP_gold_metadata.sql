CREATE OR ALTER PROCEDURE gold.log_metadata
    @table_name VARCHAR(100), 
    @start_time DATETIME2, 
    @ins INT = 0, 
    @status VARCHAR(20) = 'Success', 
    @error NVARCHAR(MAX) = NULL
AS
BEGIN
    INSERT INTO gold.metadata (table_name, load_start_time, load_end_time, rows_inserted, status, error_message)
    VALUES (@table_name, @start_time, SYSDATETIME(), @ins, @status, @error);
END;
