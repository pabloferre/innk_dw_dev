CREATE TABLE IF NOT EXISTS public.dim_goals (
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    goal_db_id INT,
    company_id
    CONSTRAINT fk_id_company FOREIGN KEY (company_id) REFERENCES dim_companies(id),
    goal_name VARCHAR(24000),
    goal_description VARCHAR(24000),
    active BOOLEAN,
    ideas_reception BOOLEAN,
    is_private BOOLEAN,
    end_campaign TIMESTAMP WITHOUT TIME ZONE,
    created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
	updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    valid_from TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    valid_to TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    is_current BOOLEAN DEFAULT TRUE
)

INSERT INYO public.dim_goals (
    goal_db_id,
    goal_name,
    goal_description,
    active,
    ideas_reception,
    is_private,
    end_campaign,
    created_at,
    updated_at,
    valid_from,
    valid_to,
    is_current
)
VALUES (
    0,
    'Sin meta',
    'No hay meta asignada',
    TRUE,
    TRUE,
    FALSE,
    '2021-01-01 00:00:00',
    '2021-01-01 00:00:00',
    '2021-01-01 00:00:00',
    '2021-01-01 00:00:00',
    TRUE
)