import datetime as dt
from ercot_api import ERCOTAPI
from streamlit import cache_data, cache_resource, subheader, table, download_button, header, date_input
from pandas import DataFrame

@cache_data
def convert_df_to_csv(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')

def get_spp_df(date:dt.date = dt.date.today()):
    try:
        api = ERCOTAPI("madison.krone@repsol.com", "Repsol123", "1df275c218464aacb4c55e05dfc12886")
        current_spp_df = api.get_dam_spp(str(date), str(date), "HB_WEST")
        print(current_spp_df)
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
