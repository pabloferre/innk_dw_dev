CREATE TABLE IF NOT EXISTS innk_dw_dev.public.dim_users (
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    company_id INT,
    user_db_id INT,
    name VARCHAR(2400),
    last_name VARCHAR(2400),
    email VARCHAR(2400),
    position VARCHAR(2400),
    contract_profile VARCHAR(2400),
    area VARCHAR(2400),
    sub_area VARCHAR(2400),
    country VARCHAR(2400),
    CONSTRAINT fk_id_company FOREIGN KEY (company_id) REFERENCES dim_company(id),
    created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    valid_from TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    valid_to TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    is_current BOOLEAN DEFAULT TRUE
)