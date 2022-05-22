from etl_helper import extrac_csv, drop_empty_columns, yn_to_char, quality_report, clean_data

## Initial ETL Workload:


def main():
    # 1-. Extract

        # CSV extraction
    app_token = "F8rh42EV3I9v9BGU2KSFk7BU6"
    csv_file = f"https://data.cityofnewyork.us/api/views/ez4e-fazm/rows.csv?$$app_token={app_token}"
    csv_dtypes = {"Number_Of_Students_On_The_Bus": "int8",
                    "Busbreakdown_ID": "int32",
                    "Run_Type": "category",
                    "Reason": "category",
                    "Has_Contractor_Notified_Schools": "category",
                    "Has_Contractor_Notified_Parents": "category",
                    "Have_You_Alerted_OPT": "category",
                    "Breakdown_or_Running_Late": "category",
                    "School_Age_or_PreK": "category",
                    }
    
    # declare columns to skip the ones you don't need
    csv_cols = ['School_Year', 'Busbreakdown_ID', 'Run_Type', 'Bus_No', 'Route_Number',
       'Reason', 'Schools_Serviced', 'Occurred_On', 'Created_On', 'Boro',
       'Bus_Company_Name', 'How_Long_Delayed', 'Number_Of_Students_On_The_Bus',
       'Has_Contractor_Notified_Schools', 'Has_Contractor_Notified_Parents',
       'Have_You_Alerted_OPT', 'Informed_On',
       'Last_Updated_On', 'Breakdown_or_Running_Late', 'School_Age_or_PreK']
    date_columns = ['Last_Updated_On', 'Informed_On','Occurred_On', 'Created_On']
    df = extrac_csv(csv_file, csv_dtypes, csv_cols, date_columns)

    # # 2-. Transform

    # Check quality metrics of the initial dataframe:
    quality_report(df)

    # drop repeated rows and and Nan Columns
    df = clean_data(df)

    # transform Yes/No columns to True/False as a way to improve speed and memory
    df = yn_to_char(df, 'Have_You_Alerted_OPT', 'Has_Contractor_Notified_Schools', 'Has_Contractor_Notified_Parents')
    
    # drop columns that are mostly empty:
    df = drop_empty_columns(df, 0.95)

    # 3-.Load:

    # vizualize:

    # load data to hdf5:
    df.to_hdf("Cleaned_Bus_Data.h5", key="df", mode="w", format="table")
    
if __name__ == "__main__":
  main()