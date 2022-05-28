from etl_helper import load, extract_first_number, check_regex_column, extrac_csv, drop_empty_columns, yn_to_char, quality_report, clean_data
import os
import socket

## Initial ETL Workload:


def main():
    # 1-. Extract

    # CSV extraction

     # token required to  extract data from data.cityofny without restrictions. if expired, obtein a new token at https://data.cityofnewyork.us/login
    app_token = "F8rh42EV3I9v9BGU2KSFk7BU6"
    # csv file address for extraction
    csv_file = f"https://data.cityofnewyork.us/api/views/ez4e-fazm/rows.csv?$$app_token={app_token}"
    # data types must be specified in order to be able to append new data to file that will be created at the end of this etl
    # it also speed up the etraction process and lessens the need for future transformations on the dataset
    # for referen on pandas datatypes, visit https://pbpython.com/pandas_dtypes.html
    csv_dtypes = {"Number_Of_Students_On_The_Bus": "int8",
                    "Busbreakdown_ID": "int64",
                    "Run_Type": "category",
                    "Reason": "category",
                    "Boro":"category",
                    "Bus_No": "object",
                    "Route_Number":"object",
                    "Bus_Company_Name": "object",
                    "School_Year": "object",
                    "Schools_Serviced": "object",
                    "Has_Contractor_Notified_Schools": "category",
                    "Has_Contractor_Notified_Parents": "category",
                    "Have_You_Alerted_OPT": "category",
                    "Breakdown_or_Running_Late": "category",
                    "School_Age_or_PreK": "category",
                    }
    
    # declare date columns to be parsed

    date_columns = ['Last_Updated_On', 'Informed_On','Occurred_On', 'Created_On']
    index_col = "Busbreakdown_ID"

    # extract csv file
    df = extrac_csv(csv_file, csv_dtypes, date_columns, index_col)

    # # 2-. Transform

    # Check quality metrics of the initial dataframe:
    quality_report(df)

    # drop repeated rows and and Nan Columns
    df = clean_data(df)

    # Transform the values in column How_Long_Delayed to keep only the first number string. Here we are assuming that the all if not most inputs were given in minutes
    df = extract_first_number(df, "How_Long_Delayed")

    # column How_Long_Delayed is renamed to Minutes_Delayed 
    df.rename({'How_Long_Delayed':'Minutes_Delayed'}, inplace=True, axis=1)

    # transform Yes/No columns to True/False as a way to improve speed and memory
    df = yn_to_char(df, 'Have_You_Alerted_OPT', 'Has_Contractor_Notified_Schools', 'Has_Contractor_Notified_Parents')
    
    # create a new column, "Service type", to collect information based on the Route_Nuber indetifier:
    df["Service_type"]= check_regex_column(df, "Route_Number")

    # drop columns that are mostly empty:
    df = drop_empty_columns(df, 0.95)

    # 3-.Load:

    # load data to hdf5:
    df.to_hdf("Bus_Breakdown_and_Delays_Data.h5", key="df", mode="w", format="table")

    #load data to postgres

    #get username and password from eviroment variable
    user = os.environ["PG_USER"]
    password = os.environ["PG_USER_PASSWORD"]

    #get hostname using socket
    host_name = socket.gethostname()   

    #load data using the load function 
    load(df, user, password, host_name, "Bus_Breakdown_and_Delays_Data")
    
if __name__ == "__main__":
  main()