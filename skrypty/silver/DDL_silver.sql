-- Silver metadata
CREATE TABLE silver.metadata (
    metadata_id INT IDENTITY(1,1) PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    source_layer VARCHAR(20) NOT NULL DEFAULT 'bronze',
    load_start_time DATETIME2 NULL,
    load_end_time DATETIME2 NULL,
    batch_id VARCHAR(50) NOT NULL,
    rows_inserted INT NULL,
    rows_updated INT NULL,
    rows_deleted INT NULL,
    scd_type VARCHAR(20) NULL, -- 'Type 1', 'Type 2', 'Type 3'
    status VARCHAR(50) NOT NULL,
    error_message VARCHAR(MAX) NULL,
    created_at DATETIME2 NOT NULL DEFAULT GETDATE()
);

-- YES - Add indexes
CREATE INDEX IX_silver_metadata_table_status 
ON silver.metadata(table_name, status, load_start_time DESC);
