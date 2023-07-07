CREATE TABLE IF NOT EXISTS innk_dw_dev.public.dim_users (
    id INT IDENTITY(1, 1) PRIMARY KEY NOT NULL UNIQUE,
    users JSON, 
    users_json INT,
    CONSTRAINT fk_id_users_json FOREIGN KEY (users_json) REFERENCES dim_users_json(id),
    created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
	updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    valid_from TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    valid_to TIMESTAMP WITHOUT TIME ZONE   ENCODE az64,
    is_current BOOLEAN DEFAULT TRUE
)