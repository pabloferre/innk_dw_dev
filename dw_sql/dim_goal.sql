CREATE TABLE IF NOT EXISTS innk_dw_dev.public.dim_goals (
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    goal_db_id INT,
    CONSTRAINT fk_id_company FOREIGN KEY (id_company) REFERENCES dim_companies(id),
    category_id INT,
    CONSTRAINT fk_id_category FOREIGN KEY (id_category) REFERENCES dim_categories(id),
    goal_name VARCHAR(24000),
    goal_description VARCHAR(24000),
    created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
	updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    valid_from TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    valid_to TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    is_current BOOLEAN DEFAULT TRUE
)