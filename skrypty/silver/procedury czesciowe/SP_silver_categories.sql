CREATE OR ALTER PROCEDURE silver.load_categories
    @silver_batch_id VARCHAR(50),
    @source_batch_id VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @table_name VARCHAR(100) = 'categories';
    DECLARE @scd_type VARCHAR(20) = 'Type 1';
    DECLARE @start_time DATETIME2 = SYSDATETIME();
    DECLARE @rows_affected INT = 0;

    BEGIN TRY
        PRINT '>> Loading: silver.' + @table_name;

        -- 1. Transformation Logic (CTE)
        WITH CleanedCategories AS (
            SELECT category_id, ISNULL(TRIM(name), 'N/A') AS category_name, parent_category_id, created_at, updated_at
            FROM bronze.categories
        )
        -- 2. Upsert Logic
        MERGE silver.categories AS target
        USING CleanedCategories AS source
        ON target.category_id = source.category_id
        WHEN MATCHED AND (
            ISNULL(target.category_name, '') <> ISNULL(source.category_name, '')
        ) THEN
            UPDATE SET
                category_name = source.category_name,
				parent_category_id = source.parent_category_id,
                source_created_at = source.created_at,
                source_updated_at = source.updated_at,
                dwh_load_date = SYSDATETIME(),
                dwh_batch_id = @silver_batch_id
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (category_id, category_name, parent_category_id, source_created_at, source_updated_at, dwh_load_date, dwh_batch_id)
            VALUES (source.category_id, source.category_name, source.parent_category_id, source.created_at, source.updated_at, SYSDATETIME(), @silver_batch_id);

        SET @rows_affected = @@ROWCOUNT;

        -- 3. Log Success
        EXEC silver.log_metadata 
            @table_name, @start_time, @silver_batch_id, @source_batch_id, 
            @ins = @rows_affected, @scd = @scd_type, @status = 'Success';

    END TRY
    BEGIN CATCH
        -- 4. Log Failure
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        EXEC silver.log_metadata 
            @table_name, @start_time, @silver_batch_id, @source_batch_id, 
            @status = 'Failed', @error = @err_msg;
            
        PRINT 'ERROR in ' + @table_name + ': ' + @err_msg;
    END CATCH
END;
