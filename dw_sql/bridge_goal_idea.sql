CREATE TABLE IF NOT EXISTS innk_dw_dev.public.bridge_goal_idea(
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    goal_id INT,
    CONSTRAINT fk_id_goal FOREIGN KEY (goal_id) REFERENCES dim_goals(id),
    CONSTRAINT fk_id_company FOREIGN KEY (id_company) REFERENCES dim_companies(id),
    idea_id INT,
    CONSTRAINT fK_id_idea FOREIGN KEY (id_idea) REFERENCES dim_ideas(id),
)