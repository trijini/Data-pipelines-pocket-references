CREATE TABLE dag_run_history (
    id INT,
    dag_id VARCHAR(250),
    execution_date TIMESTAMP WITH TIME ZONE,
    state VARCHAR(250),
    run_id VARCHAR(250),
    external_trigger BOOLEAN,
    end_date TIMESTAMP WITH TIME ZONE,
    start_date TIMESTAMP WITH TIME ZONE
);