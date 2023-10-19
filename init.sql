DROP TABLE IF EXISTS animal_dim;
CREATE TABLE animal_dim (
    animal_id VARCHAR PRIMARY KEY,
    dob DATE,
    animal_type VARCHAR,
    sterilization_status VARCHAR,
    gender VARCHAR,
    age_years VARCHAR
);

DROP TABLE IF EXISTS outcometype_dim;
CREATE TABLE outcometype_dim (
    outcome_type_key INT PRIMARY KEY,
    outcome_type VARCHAR
);

DROP TABLE IF EXISTS date_dim;
CREATE TABLE date_dim (
    date_key INT PRIMARY KEY,
    ts DATE,
    month VARCHAR,
    year VARCHAR
);

DROP TABLE IF EXISTS outcomes_fact;
CREATE TABLE outcomes_fact (
    outcome_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES date_dim(date_key),
    animal_id INT REFERENCES animal_dim(animal_id),
    outcome_type_key INT REFERENCES outcome_type_dim(outcome_type_key)
);
