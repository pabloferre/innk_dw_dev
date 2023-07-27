CREATE TABLE IF NOT EXISTS innk_dw_dev.public.dim_users_json (
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    users_db_id INT,
    company_id INT,
    CONSTRAINT fk_id_company FOREIGN KEY (company_id) REFERENCES dim_company(id),
    email VARCHAR(24000),
    name VARCHAR(24000),
    last_name VARCHAR(24000),
    position VARCHAR(24000), 
    contract_profile VARCHAR(24000),
    area VARCHAR(2400),
    sub_area VARCHAR(2400),
    country VARCHAR(240),
    created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
	updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    valid_from TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    valid_to TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    is_current BOOLEAN DEFAULT TRUE
)