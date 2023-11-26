import pandas as pd
import numpy as np
from collections import OrderedDict
from s3fs.core import S3FileSystem

# A function to convert age to years
def age_to_years(age):
    age_in_years = 0
    if isinstance(age, str):
        if 'year' in age:
            age_in_years = int(age.split()[0])
        elif 'month' in age:
            age_in_months = int(age.split()[0])
            age_in_years = age_in_months/12
        elif 'day' in age:
            age_in_days = int(age.split()[0])
            age_in_years = age_in_days/365
        

    if age_in_years < 1:
      age_in_years = 0

    return str(age_in_years)

def map_month_to_quarter(month):
    if 1 <= month <= 3:
        return 'Q1'
    elif 4 <= month <= 6:
        return 'Q2'
    elif 7 <= month <= 9:
        return 'Q3'
    else:
        return 'Q4'

def prep_data(data):
    transformed_data = data.copy()
    #transformed_data[['month_recorded', 'year_recorded']] = transformed_data['monthyear'].str.split(' ', expand=True)
    transformed_data['monthyear']= pd.to_datetime(transformed_data['monthyear'])
    # Extract month and year
    transformed_data['month_recorded'] = transformed_data['monthyear'].dt.month
    transformed_data['year_recorded'] = transformed_data['monthyear'].dt.year
    transformed_data['date_of_birth'] = pd.to_datetime(transformed_data['date_of_birth'])
    # Format the datetime object to the desired format
    transformed_data['date_of_birth']= transformed_data['date_of_birth'].dt.strftime('%Y-%m-%d')
    transformed_data['animal_name'] = transformed_data['name'].fillna('Name_less')
    transformed_data['Sex upon Outcome'] = transformed_data['sex_upon_outcome'].fillna('Unknown')
    transformed_data['Sex upon Outcome'] = transformed_data['Sex upon Outcome'].replace('Unknown', 'Unknown Unknown')
    transformed_data[['sterilization_status', 'gender']] = transformed_data['Sex upon Outcome'].str.split(' ', expand=True)
    transformed_data['Age upon Outcome'] = transformed_data['age_upon_outcome'].astype(str)
    transformed_data['age_years'] = transformed_data['Age upon Outcome'].apply(age_to_years)
    transformed_data.drop(columns = ['monthyear','name', 'Sex upon Outcome', 'Age upon Outcome', 'outcome_subtype'], axis=1, inplace=True)
    transformed_data['DateTime'] = pd.to_datetime(transformed_data['datetime'])
    transformed_data['day_of_week'] = transformed_data['DateTime'].dt.day_name()
    transformed_data['quarter_recorded'] = transformed_data['DateTime'].dt.month.apply(map_month_to_quarter)
    transformed_data['DateTime'] = pd.to_datetime(transformed_data['DateTime'])
    transformed_data['DateTime'] = transformed_data['DateTime'].dt.date
    cols_mapping = {
    'animal_id': 'animal_id',
    'datetime': 'date_recorded',
    'date_of_birth': 'dob'
    }
    transformed_data.rename(columns=cols_mapping, inplace=True)
    return transformed_data


def prep_outcometypes_dimension(new_data):
    outcome_type_dimension_data = new_data[['outcome_type']].drop_duplicates()
    outcome_type_dimension_data['outcome_type_key'] = range(1, len(outcome_type_dimension_data) + 1)
    return outcome_type_dimension_data

def prep_animal_dimension(new_data):
    animal_dimension_data = new_data[['animal_id', 'animal_name', 'dob', 'animal_type', 'sterilization_status', 'gender', 'age_years', 'breed', 'color']].drop_duplicates()
    animal_dimension_data['animal_key'] = range(1, len(animal_dimension_data) + 1)
    return animal_dimension_data

def prep_date_dimension(new_data):
    date_dimension_data = new_data[['date_recorded','day_of_week', 'month_recorded', 'quarter_recorded', 'year_recorded']].drop_duplicates()
    date_dimension_data['date_key'] = range(1, len(date_dimension_data) + 1)
    return date_dimension_data

def transform_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='Extract')
    new_data = df.copy()
    new_data = prep_data(new_data)

    dimension_animal = prep_animal_dimension(new_data)
    dimension_dates = prep_date_dimension(new_data)
    dimension_outcome_types = prep_outcometypes_dimension(new_data)

    df_fact = new_data.merge(dimension_dates, how='inner', left_on='date_recorded', right_on='date_recorded')
    df_fact = df_fact.merge(dimension_animal, how='inner', left_on='animal_id', right_on='animal_id')
    df_fact = df_fact.merge(dimension_outcome_types, how='inner', left_on='outcome_type', right_on='outcome_type')

    # Map the merged DataFrame columns to the table columns
    df_fact.rename(columns={
        'date_key': 'date_key',
        'animal_key': 'animal_key',
        'outcome_type_key': 'outcome_type_key'
    }, inplace=True)

    fct_outcomes=df_fact[['date_key', 'animal_key', 'outcome_type_key']]

    save_to_s3(dimension_animal, 'dimension_animals.csv')
    save_to_s3(dimension_dates, 'dimension_dates.csv')
    save_to_s3(dimension_outcome_types, 'dimension_outcome_types.csv')

    save_to_s3(fct_outcomes,'fact_outcomes.csv')

    return OrderedDict({'dimension_animals':dimension_animal, 
            'dimension_dates':dimension_dates,
            'dimension_outcome_types':dimension_outcome_types,
            'fct_outcomes':fct_outcomes
            })

def save_to_s3(data, file_name):
    s3_bucket = "airflow-dtsc"
    s3_path = f"s3://{s3_bucket}/{file_name}"
    aws_access_key_id = "AKIA3VIUACNUPCKQQ57X"
    aws_secret_access_key = "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"
    fs = S3FileSystem(key=aws_access_key_id, secret=aws_secret_access_key)
    with fs.open(s3_path, 'w') as file:
        data.to_csv(file, index=False)

    print(f"Saved {file_name} to S3.")