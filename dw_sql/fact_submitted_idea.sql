CREATE TABLE IF NOT EXISTS innk_dw_dev.public.fact_submitted_idea (
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    idea_id INT,
    CONSTRAINT fk_id_idea FOREIGN KEY (idea_id) REFERENCES dim_idea(id),
    company_id INT,
    CONSTRAINT fk_id_company FOREIGN KEY (company_id) REFERENCES dim_company(id),
    user_id_1 INT,
    CONSTRAINT fk_id_users FOREIGN KEY (user_id_1) REFERENCES dim_users(id),
    submitted_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    user_id_2 INT,
    CONSTRAINT fk_id_users FOREIGN KEY (user_id_2) REFERENCES dim_users(id),
    user_id_3 INT,
    CONSTRAINT fk_id_users FOREIGN KEY (user_id_3) REFERENCES dim_users(id),
    user_id_4 INT,
    CONSTRAINT fk_id_users FOREIGN KEY (user_id_4) REFERENCES dim_users(id),
    users VARCHAR(24000)
)