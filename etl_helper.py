## ETL Helper funtions

import pandas as pd
from multiprocessing import Pool
from functools import partial
import numpy as np


#extract a json file using a url
def extract_json(url, dtypes):
    print("Your Data is loading...\n")
    try:
        # load json
        df = pd.read_json(url, dtype=dtypes, convert_dates=True)
    except FileNotFoundError:
        print("File not found.")
    except pd.errors.EmptyDataError:
        print("No data found.")
    except pd.errors.ParserError:
        print("Parse error.")
    except Exception:
        print(Exception)

    else:
        print("Your Json data was loaded successfully.\n")
        return df
    return

#extract a csv file
def extrac_csv(csv_file, dtypes, cols, date_columns):
    try:
        df = pd.read_csv(csv_file, dtype=dtypes, usecols=cols, parse_dates=date_columns)
        # df = pd.read_csv(csv_file, engine="pyarrow", dtype=dtypes, usecols=cols, parse_dates=date_columns)
    except FileNotFoundError:
        print("File not found.")
    except pd.errors.EmptyDataError:
        print("No data found.")
    except pd.errors.ParserError:
        print("Parse error.")
    except Exception:
        print(Exception)
        
    else: 
        print("Your CSV data was loaded successfully.")
        return df
    return

def load(df):
    pass

def quality_report(df):
    print(f"Number of Null Values per column: \n{df.isna().sum()}")
    print(f"Duplicated Rows: {df.duplicated().sum()}")
    return

def clean_data(df):
    return df.dropna(how="all").drop_duplicates()

#function that deletes any column with more than x% of NaN values (default 95%)
def drop_empty_columns(df, treshold=0.95):
    return df.dropna(thresh=df.shape[0]*treshold,how='all',axis=1)

def yn_to_char(df, *args):
    for i in args: 
        df[i] = parallelize_on_rows(df[i], yes_or_no)
    return df

def string_to_list_parser(df, *args):
    for i in args:
        df[i] = parallelize_on_rows(df[i], string_to_list)
    return df

def yes_or_no(string):
    return "Y" if string=="Yes" else "N"

def string_to_list(string):
    l = string.rsplit(",")
    return l

def parallelize(data, func, num_of_processes=8):
    data_split = np.array_split(data, num_of_processes)
    pool = Pool(num_of_processes)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data

def run_on_subset(func, data_subset):
    return data_subset.apply(func)

def parallelize_on_rows(data, func, num_of_processes=8):
    return parallelize(data, partial(run_on_subset, func), num_of_processes)

def nycounties_to_fips_dict(county):
  fips = {
      "Manhattan": "36061",
      "Bronx": "36005",
      "Brooklyn": "36047",
      "Staten Island": "36085",
      "Queens": "36081",
      "Nassau County": "36059",
      "Westchester":"36119",
      "Rockland County": "36087",
      "Connecticut": "09",
      "New Jersey": "34",
      
  }
  return fips[county] if fips.get(county) else np.nan

def nycounties_to_fips(df, row, new_row):
    df[new_row] = parallelize_on_rows(df[row], nycounties_to_fips_dict)
    return df

# def save_to_sqlite(df):
#     cnx = sqlite3.connect(':memory:')
#     df.to_sql(name='Cleaned_Bus_Breakdown_Data', con=cnx, if_exists='append')