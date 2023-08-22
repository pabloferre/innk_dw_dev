CREATE TABLE IF NOT EXISTS innk_dw_dev.public.param_clustered_ideas (
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    idea_id INT,
    CONSTRAINT fk_id_idea FOREIGN KEY (idea_id) REFERENCES dim_idea(id),
    idea_db_id INT,
    company_id INT,
    CONSTRAINT fk_id_company FOREIGN KEY (company_id) REFERENCES dim_company(id),
    goal_id INT,
    CONSTRAINT fk_id_goal FOREIGN KEY (goal_id) REFERENCES dim_goal(id),
    idea_c_name VARCHAR(24000),
    idea_c_description VARCHAR(24000),
    cluster_name VARCHAR(24000),
    cluster_description VARCHAR(24000),
    cluster_number INT
)