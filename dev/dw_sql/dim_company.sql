CREATE TABLE IF NOT EXISTS innk_dw_dev.public.dim_company (
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    company_db_id INT,
    company_name VARCHAR(24000) NOT NULL,
    status_active BOOLEAN NOT NULL,
    category VARCHAR(2400),
    country VARCHAR(2400),
    created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
	updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    valid_from TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    valid_to TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    is_current BOOLEAN DEFAULT TRUE
)