#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import argparse 
from sqlalchemy import create_engine


def extract_data(source):
    return pd.read_csv(source)

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

def transform_data(data):
    new_data = data.copy()

    new_data = new_data.drop_duplicates(subset='Animal ID', keep='first')
    new_data = new_data.drop_duplicates(subset='DateTime', keep='first')

    # Handling incorrect values in Name column
    new_data['Name'] = new_data['Name'].mask(df['Name'] == df['Animal ID'], np.nan)
    new_data[['month', 'year']] = new_data.MonthYear.str.split(' ', expand=True)
    new_data['Sex upon Outcome'] = new_data['Sex upon Outcome'].fillna('Unknown')
    new_data['Sex upon Outcome'] = new_data['Sex upon Outcome'].replace('Unknown', 'Unknown Unknown')
    new_data[['sterilization_status', 'gender']] = new_data['Sex upon Outcome'].str.split(' ', expand=True)
    new_data['Age upon Outcome'] = new_data['Age upon Outcome'].astype(str)
    new_data['age_years'] = new_data['Age upon Outcome'].apply(age_to_years)
    new_data.drop(columns = ['MonthYear', 'Sex upon Outcome', 'Age upon Outcome'], inplace=True)
    new_data['DateTime'] = pd.to_datetime(new_data['DateTime'])
    new_data['DateTime'] = new_data['DateTime'].dt.date
    new_data['Date of Birth'] = new_data['Date of Birth'].dt.date
    mapping = {
    'Animal ID': 'animal_id',
    'Name': 'animal_name',
    'DateTime': 'ts',
    'Date of Birth': 'dob',
    'Outcome Type': 'outcome_type',
    'Outcome Subtype': 'outcome_subtype',
    'Animal Type': 'animal_type',
    'Breed': 'breed',
    'Color': 'color'
    }
    new_data.rename(columns=mapping, inplace=True)
    return new_data

def outcometype_dim_data(new_data):
    outcometype_dim = new_data[['outcome_type']].drop_duplicates()
    outcometype_dim['outcome_type_key'] = range(1, len(outcometype_dim) + 1)
    return outcometype_dim

def animaltype_data(new_data):
    animal_dim = new_data[['animal_id', 'dob', 'animal_type', 'sterilization_status', 'gender', 'age_years']].drop_duplicates()
    animal_dim['animal_key'] = range(1, len(animal_dim) + 1)
    return animal_dim

def date_dim_data(new_data):
    date_dim_data = new_data[['ts','month', 'year']].drop_duplicates()
    date_dim_data['date_key'] = range(1, len(date_dim_data) + 1)
    return date_dim_data



def load_data(data):

    animal_dim = animaltype_data(new_data)
    date_dim = date_dim_data(new_data)
    outcometype_dim = outcometype_dim_data(new_data)


    db_url="postgresql+psycopg2://anilareddy:blabla@db:5432/shelter"
    conn = create_engine(db_url)
    animal_dim.to_sql("animal_dim", conn, if_exists="append", index=False)
    date_dim.to_sql("date_dim", conn, if_exists="append", index=False)
    outcometype_dim.to_sql("outcometype_dim", conn, if_exists="append", index=False)

    outcome_fact = new_data.merge(date_dim, how='inner', left_on='ts', right_on='ts')
    outcome_fact = outcome_fact.merge(animal_dim, how='inner', left_on='animal_id', right_on='animal_id')
    outcome_fact = outcome_fact.merge(outcometype_dim, how='inner', left_on='outcome_type', right_on='outcome_type')

    # Map the merged DataFrame columns to the table columns
    outcome_fact.rename(columns={
        'date_key': 'date_key',
        'animal_key': 'animal_key',
        'outcome_type_key': 'outcome_type_key',
        'outcome_count': 'outcome_count'
    }, inplace=True)


    # Create or append data to the Outcomes_Fact table
    outcome_fact[['date_key', 'animal_id', 'outcome_type_key']].to_sql('outcomes_fact', conn, if_exists='append', index=False)

    


if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    #parser.add_argument('target', help='target csv')
    args = parser.parse_args()

    print("Starting...")
    df = extract_data(args.source)
    new_data = transform_data(df)
    load_data(new_data)
    print("Complete")