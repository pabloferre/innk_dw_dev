CREATE TABLE IF NOT EXISTS innk_dw_dev.public.dim_categories (
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    id_category_db INT,
    category_name VARCHAR(24000) NOT NULL,
    category_description VARCHAR(24000),
    company_id INT,
    CONSTRAINT fk_id_company FOREIGN KEY (company_id) REFERENCES dim_companies(id),
    status_active BOOLEAN NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64,
	updated_at TIMESTAMP WITHOUT TIME ZONE ENCODE az64
)