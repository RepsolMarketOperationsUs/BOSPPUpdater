import datetime as dt
from ercotapi import ERCOTAPI
from ercotapi import USERNAME
from ercotapi import PASSWORD
from ercotapi import API_KEY
from streamlit import cache_data, cache_resource, subheader, table, download_button, header, date_input
from pandas import DataFrame

@cache_data
def convert_df_to_csv(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

def get_spp_df(date:dt.date = dt.date.today()):
    try:
        api = ERCOTAPI(USERNAME, PASSWORD, API_KEY)
        current_spp_df = DataFrame(api.get_json_dict(api.get_dam_spp(deliveryDateFrom=str(date), deliveryDateTo=str(date), settlementPoint="HB_WEST"))["data"], 
                                    columns=["Date", "Hour Ending", "Settlement Point", "SPP", "Repeat Flag"])
        current_spp_df["Hour Ending"] = [int(x[:-3]) for x in current_spp_df["Hour Ending"]]
        current_spp_df.drop("Repeat Flag", inplace=True, axis=1)
    except KeyError:
        get_spp_df(date)
        return None
    
    subheader("Currently Showing Data For: " + str(current_spp_df["Date"].iloc[0]))
    table(current_spp_df)
    download_button(label="Download Data As CSV", data=convert_df_to_csv(current_spp_df),file_name="HB_WEST_SPP_" + str(current_spp_df["Date"].iloc[0]) + ".csv",mime='text/csv',)

header("West Hub DAM SPP Downloader")
subheader("Instructions:\n1. Select Date From Calendar Below\n3. Wait Until New Table Appears\n4. Click 'Download Data As CSV'")
date = date_input("Select Date To Download", value="today", min_value=dt.date(2024,1,1), max_value=dt.date.today() + dt.timedelta(days=1))
get_spp_df(date)