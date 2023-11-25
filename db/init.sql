DROP TABLE IF EXISTS dimension_animals CASCADE;
CREATE TABLE dimension_animals(
    animal_key INT PRIMARY KEY,
    animal_id VARCHAR,
    name VARCHAR,
    dob DATE,
    animal_type VARCHAR,
    sterilization_status VARCHAR,
    gender VARCHAR,
    age_years VARCHAR,
    breed VARCHAR,
    color VARCHAR
);

DROP TABLE IF EXISTS dimension_outcome_types CASCADE;
CREATE TABLE dimension_outcome_types(
    outcome_type_key INT PRIMARY KEY,
    outcome_type VARCHAR
);

DROP TABLE IF EXISTS dimension_dates CASCADE;
CREATE TABLE dimension_dates(
    date_key INT PRIMARY KEY,
    date_recorded DATE,
    day_of_week VARCHAR,
    month_recorded VARCHAR,
    quarter_recorded VARCHAR,
    year_recorded VARCHAR
);

DROP TABLE IF EXISTS fct_outcomes CASCADE;
CREATE TABLE fct_outcomes(
    outcome_key INT PRIMARY KEY,
    date_key INT REFERENCES dimension_dates(date_key),
    animal_key INT REFERENCES dimension_animals(animal_key),
    outcome_type_key INT REFERENCES dimension_outcome_types(outcome_type_key)
);
