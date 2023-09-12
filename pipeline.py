import pandas as pd
import numpy as np
from argparse import ArgumentParser
import argparse


def extract_data(source):
    return pd.read_csv(source, encoding='latin-1', engine='python')

def transform_data(data):
    data_n=data.copy()
    state=data_n['airport_name']
    l=[]
    for i in state:  
        between_a_and_b = i[i.index(",") + 1: i.index(":")]
        l.append(between_a_and_b)
    data_n['state']=l
    data_n.drop(columns=["airport_name", "carrier_name"], inplace=True)
    return data_n

def load_data(data, target):
    data.to_csv(target)


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    parser.add_argument('target', help='target csv')
    args= parser.parse_args()
    
    print("starting..")
    df = extract_data(args.source)
    new_df=transform_data(df)
    load_data(new_df, args.target)
    print("complete")




