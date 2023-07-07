CREATE TABLE IF NOT EXISTS innk_dw_dev.public.fact_goals (
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    id_goal_db INT,
    company_id INT,
    CONSTRAINT fk_id_company FOREIGN KEY (id_company) REFERENCES dim_companies(id),
    category_id INT,
    CONSTRAINT fk_id_category FOREIGN KEY (id_category) REFERENCES dim_categories(id),
    goal_name VARCHAR(24000),
    goal_description VARCHAR(24000),
    created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
	updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)