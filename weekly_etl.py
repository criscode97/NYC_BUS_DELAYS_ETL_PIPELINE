from etl_helper import  load, extract_json, quality_report, clean_data, check_regex_column, drop_empty_columns, yn_to_char, extract_first_number
import datetime
import os
import socket

#weekly etl
import datetime
def main():

    # 1-. Extract

    # declare url for json extraction
    app_token = "F8rh42EV3I9v9BGU2KSFk7BU6"
    row_limit = 50000 #Maximun number is 50000
    my_date = datetime.date.today()
    year, week_num, day_of_week = my_date.isocalendar()
    url = f"https://data.cityofnewyork.us/resource/ez4e-fazm.json/?$$app_token={app_token}&$limit={row_limit}&$where=date_extract_woy(created_on)={week_num-1}"

    # declare df datatypes to save memory
    json_dtypes = {"number_of_students_on_the_bus": "int8",
                   "school_year" : "object",
                    "busbreakdown_id": "int64",
                    "run_type": "category",
                    "reason": "category",
                    "boro":"category",
                    "bus_no":"object",
                    "route_number": "object",
                    "schools_serviced": "object",
                    "bus_company_name": "object",
                    "has_contractor_notified_schools": "category",
                    "has_contractor_notified_parents": "category",
                    "have_you_alerted_opt": "category",
                    "breakdown_or_running_late": "category",
                    "school_age_or_preK": "category",
                    }
    #invoke json extraction funtion
    date_columns = ['last_updated_on', 'informed_on','occurred_on', 'created_on']

    index_col = "busbreakdown_id"
  
    df = extract_json(url, json_dtypes, index_col, date_columns)

      # # 2-. Transform

    # Check quality metrics of the initial dataframe:
    quality_report(df)

    # drop repeated rows and and Nan Columns
    df = clean_data(df)

    # # drop 
    df.drop('incident_number', axis=1)

    # Transform the values in column How_Long_Delayed to keep only the first number string. Here we are assuming that the all if not most inputs were given in minutes
    df = extract_first_number(df, "how_long_delayed")
    
    # rename columns
    new_column_names = {"school_year":'School_Year',
          "busbreakdown_id":  'Busbreakdown_ID',
          "run_type": 'Run_Type',
          "bus_no": 'Bus_No',
          'route_number': 'Route_Number',
          'reason': 'Reason',
          'schools_serviced': 'Schools_Serviced',
          'occurred_on': 'Occurred_On',
          'created_on': 'Created_On',
          'boro': 'Boro',
          'bus_company_name': 'Bus_Company_Name',
          'how_long_delayed': 'Minutes_Delayed',
          'number_of_students_on_the_bus': 'Number_Of_Students_On_The_Bus',
          'has_contractor_notified_schools': 'Has_Contractor_Notified_Schools',
          'has_contractor_notified_parents': 'Has_Contractor_Notified_Parents',
          'have_you_alerted_opt': 'Have_You_Alerted_OPT',
          'informed_on': 'Informed_On',
          'last_updated_on': 'Last_Updated_On',
          'breakdown_or_running_late': 'Breakdown_or_Running_Late',
          'school_age_or_prek': 'School_Age_or_PreK'}

    # all columns will be rename to match the column on the database is renamed to Minutes_Delayed
    df.rename(new_column_names, axis=1, inplace=True)
    

    # transform Yes/No columns to True/False as a way to improve speed and memory
    df = yn_to_char(df, 'Have_You_Alerted_OPT', 'Has_Contractor_Notified_Schools', 'Has_Contractor_Notified_Parents')
    
    # create a new column, "Service type", to collect information based on the Route_Nuber indetifier:
    df["Service_type"]= check_regex_column(df, "Route_Number")

    # 3-.Load:
    

    # load data to hdf5:
    df.to_hdf("Bus_Breakdown_and_Delays_Data.h5", key="df", mode='a', format="table")

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
