import pandas as pd 

def extract_data(file_path):

    data = pd.read_csv(file_path)
    return data

data = extract_data('data/source_data.csv')
print(data.head())

def transform_data(data):
    data = data.dropna()  

    data = data[data['0'] > 18]

    return data

transform_data = transform_data(data)
print(transform_data.head())