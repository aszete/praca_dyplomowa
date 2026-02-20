/*
===============================================================================
Procedura składowana: Ładowanie warstwy Silver - procedura nadrzędna
===============================================================================
Cel:
Procedura automatyzuje ładowanie danych z plików źródłowych OLTP 
(pliki CSV) do warstwy Bronze. Dodatkowo rejestruje metryki łądowania w
tabeli metadata.

Procedura implementuje początkowy etap pobierania Medallion Architektura:
Źródło OLTP → Bronze (surowy) → Silver (oczyszczony) → Gold (wymiarowy)

Parametry:
@batch_id VARCHAR(50) — opcjonalny identyfikator partii.
                        Jeśli wartość jest równa NULL, generowany jest
                        unikalny identyfikator na podstawie bieżącego
                        znacznika czasu systemu.

Sposób użycia:
-- Ładowanie wszystkich tabel z automatycznie wygenerowanym identyfikatorem:

EXEC silver.load_full;

-- Ładowanie wybranych tabele z określonym identyfikatorem:

EXEC silver.load_full @batch_id = 'BATCH_2024_001';
===============================================================================
*/


CREATE OR ALTER PROCEDURE silver.load_full
    @silver_batch_id VARCHAR(50) = NULL,
    @source_batch_id VARCHAR(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    SET @silver_batch_id = ISNULL(@silver_batch_id, 'SILVER_' + FORMAT(SYSDATETIME(), 'yyyyMMdd_HHmmss'));
    
    IF @source_batch_id IS NULL
    BEGIN
        SELECT TOP 1 @source_batch_id = batch_id 
        FROM bronze.metadata 
        WHERE status = 'Success' 
        ORDER BY load_end_time DESC;
        
        -- Fallback
        SET @source_batch_id = ISNULL(@source_batch_id, 'INITIAL_LOAD');
    END

    PRINT '================================================';
    PRINT 'START LADOWANIA SILVER BATCH: ' + @silver_batch_id;
    PRINT 'ZRODLO BATCH:    ' + @source_batch_id;
    PRINT '================================================';

    -- 2. WYMIARY
    PRINT '>> Ladowanie tabeli wymiarów...';
    BEGIN TRY EXEC silver.load_addresses @silver_batch_id, @source_batch_id; PRINT 'OK Addresses'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Addresses'; END CATCH

    BEGIN TRY EXEC silver.load_brands @silver_batch_id, @source_batch_id; PRINT 'OK Brands'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Brands'; END CATCH

    BEGIN TRY EXEC silver.load_categories @silver_batch_id, @source_batch_id; PRINT 'OK Categories'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Categories'; END CATCH

    BEGIN TRY EXEC silver.load_customers @silver_batch_id, @source_batch_id; PRINT 'OK Customers'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Customers'; END CATCH

    BEGIN TRY EXEC silver.load_payment_methods @silver_batch_id, @source_batch_id; PRINT 'OK Payment Methods'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Payment Methods'; END CATCH

    BEGIN TRY EXEC silver.load_products @silver_batch_id, @source_batch_id; PRINT 'OK Products'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Products'; END CATCH

    -- 3. FAKTY
    PRINT '>> Ladowanie tabeli faktow...';
    BEGIN TRY EXEC silver.load_website_sessions @silver_batch_id, @source_batch_id; PRINT 'OK Sessions'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Sessions'; END CATCH

    BEGIN TRY EXEC silver.load_pageviews @silver_batch_id, @source_batch_id; PRINT 'OK Pageviews'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Pageviews'; END CATCH

    BEGIN TRY EXEC silver.load_orders @silver_batch_id, @source_batch_id; PRINT 'OK Orders'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Orders'; END CATCH

    BEGIN TRY EXEC silver.load_order_items @silver_batch_id, @source_batch_id; PRINT 'OK Order Items'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Order Items'; END CATCH

    BEGIN TRY EXEC silver.load_order_item_returns @silver_batch_id, @source_batch_id; PRINT 'OK Returns'; END TRY 
    BEGIN CATCH PRINT 'NIEUDANE: Returns'; END CATCH

    PRINT '================================================';
    PRINT 'SILVER LOAD COMPLETE';
    PRINT '================================================';

    -- 4. Podsumowanie
    SELECT 
        table_name, 
        status, 
        rows_inserted, 
        rows_updated, 
        DATEDIFF(SECOND, load_start_time, load_end_time) AS duration_sec,
        error_message
    FROM silver.metadata 
    WHERE silver_batch_id = @silver_batch_id
    ORDER BY load_start_time;
END;
