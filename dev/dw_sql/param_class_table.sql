CREATE TABLE IF NOT EXISTS innk_dw_dev.public.param_class_table (
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    company_id INT,
    field_id INT,
    CONSTRAINT fk_id_company FOREIGN KEY (company_id) REFERENCES dim_company(id),
    name VARCHAR(24000),
    description VARCHAR(24000),
    category VARCHAR(24000)
)