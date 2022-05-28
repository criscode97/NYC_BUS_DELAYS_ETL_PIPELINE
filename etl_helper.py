## ETL Helper funtions

import pandas as pd
from multiprocessing import Pool
from functools import partial
import numpy as np
import re
from sqlalchemy import create_engine

#extract a json file using a url
def extract_json(url, dtypes, index_col, date_columns):
    print("Your Data is loading...\n")
    try:
        # load json
        df = pd.read_json(url, dtype=dtypes, convert_dates=date_columns)
        df = df.set_index(index_col)
    except FileNotFoundError:
        print("File not found.")
    except pd.errors.EmptyDataError:
        print("No data found.")
    except pd.errors.ParserError:
        print("Parse error.")
    except Exception:
        print("Unkown Error.")

    else:
        print("Your Json data was loaded successfully.\n")
        return df
    return

#extract a csv file
def extrac_csv(csv_file, dtypes, date_columns, index_col):
    print("Your Data is loading...\n")
    try:
      df = pd.read_csv(csv_file, dtype=dtypes, parse_dates=date_columns)
    # ---- THIS EXTRACTION METHOD USES PYARROW TO SPEED THE STRACTION PROCESS BY 7X, HOWEVER, I WASN'T ABLE TO MAKE IT WORK ON COLABORATE
    # df = pd.read_csv(csv_file, engine="pyarrow", dtype=dtypes, parse_dates=date_columns, index_col=index_col)
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

# Print custome quality reports using the pandas library
def quality_report(df):
    r, c = df.shape
    print(f""" DATA QUALITY REPORT:
    Columns:{c}
    Rows:{r}
    Number of Null Values per column: \n{df.isna().sum()}")
    Duplicated Rows: {df.duplicated().sum()}""")
    return

# quick data cleanup to be ran after the quality of the data has been analize
def clean_data(df):
    return df.dropna(how="all").drop_duplicates()

#function that deletes any column with more than x% of NaN values (default 95%)
def drop_empty_columns(df, treshold=0.95):
    return df.dropna(thresh=df.shape[0]*treshold,how='all',axis=1)

# this funtion takes a dataframe and a colection of argument corresponding to the names of data frame which must only contain yes or no values
def yn_to_char(df, *args):
    # for each column name passed
    for i in args: 
        # transform column using parallelism with the funtion 'yes_or_no'
        df[i] = parallelize_on_rows(df[i], yes_or_no)
    # return transformed dataframe
    return df

# this funtion take a Yes or No string argument and return Y or N, allowing for memory optimization
def yes_or_no(string):
    return "Y" if string=="Yes" else "N"

# this funtion allows for a data tranformation to be parallelize with 8 workers using python-native multiprocessing library
def parallelize(data, func, num_of_processes=8):
    data_split = np.array_split(data, num_of_processes)
    pool = Pool(num_of_processes)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data

# this funtion takes subsets of a column to be simultiniously transformed
def run_on_subset(func, data_subset):
    return data_subset.apply(func)

# this funtion divides a column into subsets using the run_on_subset function and simultaniously tranfroming it using parallelism with the parallelize function
def parallelize_on_rows(data, func, num_of_processes=8):
    return parallelize(data, partial(run_on_subset, func), num_of_processes)

# this dictionary is used on the regex_checker funtion by default to map the regex string to the corresponding service type
route_regex = {
    "^[a-zA-Z]([0-9]{3})$" : "curb-to-curb",
    "^[a-zA-Z]([0-9]{4})$" : "stop-to-school",
    "(.*?)" : "Pre-K/EI routes"
}

# this function takes for arguments a string and a dictionary that maps regex expressions with their corresponding value (default route_regex)
# the funtion return the value string that corresponds to the matching regex key 
def regex_checker(string, identifier=route_regex):
  for i in identifier:
    pattern = re.compile(i)
    if pattern.match(str(string)):
        return identifier[i]

# the function takes takes a data frame and a column name, checking each string in the column using the regex_checker funtion
def check_regex_column(df, column):
    return parallelize_on_rows(df[column], regex_checker)

# this funtion takes as argument a string and regex expresion, return the part of the string that matches the the regex if a match is found,
# else, it returns the string passes
def regex_number_finder(string, regex="(?m)^(\d+).*"):
    pattern = re.compile(regex)
    m = pattern.search(str(string))
    return m.group(1) if m else string

# this funtion applies the regex_number_finder to the given dataframe at the psecified column using parallelism
def extract_first_number(df, column):
      df[column] = parallelize_on_rows(df[column], regex_number_finder)
      return df

def load(df, user, password, server, table):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{user}:{password}@{server}:5432/{table}')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {table}')
        # save df to postgres
        df.to_sql(f'stg_{table}', engine, if_exists='append', index=False)
        rows_imported += len(df)
        # add elapsed time to final print out
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))  



# --------------- This funtions were created but later unused, though the might be helpful for future analisys  ---------------------

## this funtion takes a string with comas a returns a list of values in between each coma
# def string_to_list(string):
#     return string.rsplit(",")

## This function applies the string_to_list function to the given dataframe at the psecified column using parallelism
# def string_to_list_parser(df, *args):
#     for i in args:
#         df[i] = parallelize_on_rows(df[i], string_to_list)
#     return df

## this funtion was created for the values on the Boro column on the the Bus_Breakdowns_And_Delays dataset. It takes a string in the column
## and returns a fips (numbers which uniquely identify geographic areas) with the purpose of creating geografical maps for data visualization
# def nycounties_to_fips_dict(county):
#   fips = {
#       "Manhattan": "36061",
#       "Bronx": "36005",
#       "Brooklyn": "36047",
#       "Staten Island": "36085",
#       "Queens": "36081",
#       "Nassau County": "36059",
#       "Westchester":"36119",
#       "Rockland County": "36087",
#       "Connecticut": "09",
#       "New Jersey": "34"}
#   return fips[county] if fips.get(county) else np.nan

## This function applies the nycounties_to_fips_dict function to the given dataframe at the psecified column using parallelism
# def nycounties_to_fips(df, row, new_row):
#     df[new_row] = parallelize_on_rows(df[row], nycounties_to_fips_dict)
#     return df
