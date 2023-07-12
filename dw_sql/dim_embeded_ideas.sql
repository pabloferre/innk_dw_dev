CREATE TABLE IF NOT EXISTS innk_dw_dev.public.dim_embedded_idea (
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    idea_id INT,
    CONSTRAINT fk_id_idea FOREIGN KEY (idea_id) REFERENCES dim_idea(id),
    idea_db_id INT NOT NULL,
    problem_embedded VARCHAR(24000),
    solution_embedded VARCHAR(24000),
    created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
)
)