from etl_helper import  extract_json, yn_to_bool
import datetime
def main():

    # 1-. Extract

        # declare url for jsnon extraction
    app_token = "F8rh42EV3I9v9BGU2KSFk7BU6"
    row_limit = 50000 #Maximun number is 50000
    current_week = datetime.date.today().isocalendar().week
    url = f"https://data.cityofnewyork.us/resource/ez4e-fazm.json/?$$app_token={app_token}&$limit={row_limit}&$where=date_extract_woy(created_on)={current_week}"

        # declare df datatypes to save memory
    json_dtypes = {"number_of_students_on_the_bus": "int8",
                    "busbreakdown_id": "int32",
                    "run_type": "category",
                    "reason": "category",
                    "has_contractor_notified_schools": "category",
                    "has_contractor_notified_parents": "category",
                    "have_you_alerted_opt": "category",
                    "breakdown_or_running_late": "category",
                    "school_age_or_preK": "category",
                    'occurred_on': "datetime64[ns]",
                    'created_on': "datetime64[ns]",
                    'informed_on': "datetime64[ns]",
                    'last_updated_on': "datetime64[ns]"
                    }
        #invoke json extraction funtion
    df = extract_json(url, json_dtypes)

    df = yn_to_bool(df, "have_you_alerted_opt", "has_contractor_notified_parents", "has_contractor_notified_schools")

    #  load data to an existing hdf5 file:
    df.to_hdf("Cleaned_Bus_Breakdown_Data.h5", append=True, key="df", mode='a')


if __name__ == "__main__":
    main()
