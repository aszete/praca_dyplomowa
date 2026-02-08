/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
*/

TRUNATE TABLE bronze.payment_methods;

BULK INSERT bronze.payment_methods
FROM 'C:\Projekt dyplomowy\dane\bronze.payment_methods.csv'
WITH (
    FIRSTROW = 2,              -- skip header
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    CODEPAGE = '65001',        -- UTF-8 (important for Polish chars!)
    TABLOCK
);
