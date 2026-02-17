CREATE OR ALTER PROCEDURE silver.load_products
    @silver_batch_id VARCHAR(50) = NULL,
    @source_batch_id VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Auto-generate batch IDs if not provided (though orchestrator should always provide them)
    SET @silver_batch_id = ISNULL(@silver_batch_id, 'AUTO_' + FORMAT(SYSDATETIME(), 'yyyyMMddHHmmss'));
    SET @source_batch_id = ISNULL(@source_batch_id, @silver_batch_id);
    
    DECLARE @table_name VARCHAR(100) = 'products';
    DECLARE @start_time DATETIME2 = SYSDATETIME();
    DECLARE @current_time DATETIME2 = SYSDATETIME();
    
    DECLARE @rows_inserted INT = 0;
    DECLARE @rows_updated INT = 0;
    
    BEGIN TRY
        PRINT '>> Processing SCD Type 2: silver.' + @table_name;
        
        -- ============================================================
        -- STEP 1: Transform and clean source data into temp table
        -- ============================================================
        IF OBJECT_ID('tempdb..#CleanedProducts') IS NOT NULL DROP TABLE #CleanedProducts;
        
        SELECT 
            product_id, 
            ISNULL(TRIM(name), 'N/A') AS product_name,
            ISNULL(TRIM(REPLACE(description, '%', ' ')), 'N/A') AS description,
            category_id, 
            brand_id, 
            ABS(list_price) AS list_price,
            created_at, 
            updated_at
        INTO #CleanedProducts
        FROM bronze.products;
        
        -- ============================================================
        -- STEP 2: Identify change types into temp table
        -- ============================================================
        IF OBJECT_ID('tempdb..#ChangedRecords') IS NOT NULL DROP TABLE #ChangedRecords;
        
        SELECT 
            cp.*,
            sp.product_skey,
            sp.list_price AS old_list_price,
            CASE 
                -- New product (doesn't exist in silver)
                WHEN sp.product_skey IS NULL THEN 'INSERT'
                
                -- Price changed (SCD Type 2 attribute)
                WHEN cp.list_price <> sp.list_price THEN 'SCD2_UPDATE'
                
                -- Other attributes changed (optional: you can add Type 1 logic here)
                WHEN cp.product_name <> sp.product_name 
                  OR cp.description <> sp.description
                  OR ISNULL(cp.category_id, -1) <> ISNULL(sp.category_id, -1)
                  OR ISNULL(cp.brand_id, -1) <> ISNULL(sp.brand_id, -1) 
                THEN 'TYPE1_UPDATE'
                
                -- No changes
                ELSE 'NO_CHANGE'
            END AS change_type
        INTO #ChangedRecords
        FROM #CleanedProducts cp
        LEFT JOIN silver.products sp 
            ON cp.product_id = sp.product_id 
            AND sp.is_current = 1;  -- Only compare with current records
        -- ============================================================
        -- STEP 3: Close out old records (expire them)
        -- ============================================================
        UPDATE sp
        SET 
            valid_to = @current_time,
            is_current = 0
        FROM silver.products sp
        INNER JOIN #ChangedRecords cr 
            ON sp.product_id = cr.product_id
            AND sp.is_current = 1
        WHERE cr.change_type = 'SCD2_UPDATE';
        
        SET @rows_updated = @@ROWCOUNT;
        
        IF @rows_updated > 0
            PRINT '   [SCD2] Expired ' + CAST(@rows_updated AS VARCHAR) + ' old record(s)';
        -- ============================================================
        -- STEP 4: Insert new records (new products + new versions)
        -- ============================================================
        INSERT INTO silver.products (
            product_id, 
            product_name, 
            description, 
            category_id, 
            brand_id, 
            list_price,
            valid_from, 
            valid_to, 
            is_current, 
            source_created_at, 
            source_updated_at, 
            dwh_load_date, 
            dwh_batch_id
        )
        SELECT 
            product_id,
            product_name,
            description,
            category_id,
            brand_id,
            list_price,
            @current_time AS valid_from,
            NULL AS valid_to,
            1 AS is_current,
            created_at,
            updated_at,
            @current_time AS dwh_load_date,
            @silver_batch_id
        FROM #ChangedRecords
        WHERE change_type IN ('INSERT', 'SCD2_UPDATE');  -- Insert new and changed records
        
        SET @rows_inserted = @@ROWCOUNT;
        
        IF @rows_inserted > 0
            PRINT '   [SCD2] Inserted ' + CAST(@rows_inserted AS VARCHAR) + ' new version(s)';
        -- ============================================================
        -- STEP 5: Handle Type 1 updates (overwrite in place)
        -- Optional: Update non-tracked attributes without creating new version
        -- ============================================================
        UPDATE sp
        SET 
            product_name = cr.product_name,
            description = cr.description,
            category_id = cr.category_id,
            brand_id = cr.brand_id,
            source_updated_at = cr.updated_at,
            dwh_load_date = @current_time,
            dwh_batch_id = @silver_batch_id
        FROM silver.products sp
        INNER JOIN #ChangedRecords cr 
            ON sp.product_id = cr.product_id
            AND sp.is_current = 1
        WHERE cr.change_type = 'TYPE1_UPDATE';
        
        DECLARE @type1_updates INT = @@ROWCOUNT;
        
        IF @type1_updates > 0
            PRINT '   [Type1] Updated ' + CAST(@type1_updates AS VARCHAR) + ' record(s) in place';
        
        -- ============================================================
        -- Cleanup temp tables
        -- ============================================================
        DROP TABLE IF EXISTS #CleanedProducts;
        DROP TABLE IF EXISTS #ChangedRecords;
        -- ============================================================
		-- STEP 6: Handle deletions (products removed from source)
		-- ============================================================
		UPDATE sp
		SET 
			valid_to = @current_time,
			is_current = 0
		FROM silver.products sp
		WHERE sp.is_current = 1
		  AND NOT EXISTS (
			  SELECT 1 
			  FROM #CleanedProducts cp 
			  WHERE cp.product_id = sp.product_id
		  );

		DECLARE @rows_deleted INT = @@ROWCOUNT;

		IF @rows_deleted > 0
			PRINT '   [Deleted] Closed out ' + CAST(@rows_deleted AS VARCHAR) + ' discontinued product(s)';
        -- ============================================================
        -- STEP 7: Log metadata
        -- ============================================================
        EXEC silver.log_metadata 
            @table_name, 
            @start_time, 
            @silver_batch_id, 
            @source_batch_id, 
            @ins = @rows_inserted, 
            @upd = @rows_updated, 
            @scd = 'Type 2', 
            @status = 'Success';
        
        PRINT 'SCD Type 2 processing complete';
        
    END TRY
    BEGIN CATCH
        DECLARE @err_msg NVARCHAR(MAX) = ERROR_MESSAGE();
        PRINT '!! ERROR: ' + @err_msg;
        
        EXEC silver.log_metadata 
            @table_name, 
            @start_time, 
            @silver_batch_id, 
            @source_batch_id, 
            @status = 'Failed', 
            @error = @err_msg;
        
        -- Optionally re-throw the error
        THROW;
    END CATCH
END;
GO
