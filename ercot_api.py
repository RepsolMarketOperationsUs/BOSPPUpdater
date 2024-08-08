"""
Summary
-------
Pull data from ERCOT API and store into standardized DataFrame structure.

Imports
-------
Inspect
Pandas
Requests

Classes
-------
ERCOTAPI

Author
------
Brayden Libby 
Email: brayden_libby@protonmail.com
Linkedin: https://linkedin.com/in/brayden-libby
GitHub: https://github.com/BNLibby
"""

"""
TODO: 
    DAM
        LMP
        Total AS
        Total MW Purchased
        Total MW Sold
    RTM
        Actual Hourly Solar
        Actual Hourly Wind
        ORDC PA
        Actual System Wide Load
        Actual Load By WZ
        Actual Load By FZ
        Forecasted Load by WZ
        Forecasted Load by FZ
        LMP
            Bus
            Zone
        SPP 
            Hub
            LZ
            RN 
"""

# Standard Imports
from requests import Response, post, get
import pandas as pd
import time
import datetime as dt
from numpy import nan
import urllib3

# Ignore warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ERCOTAPI:
    """
    Summary
    -------
    Class that represents a connection to the ERCOT API.

    Attributes
    ----------
    __username: str
        Username for the ERCOT API connection.
    __password: str
        Password for the ERCOT API connection.
    __api_key: str
        API key for the ERCOT API connection.
    __access_token: str
        Access token for ERCOT API requests.
    __authentication_header: str
        Authentication header for ERCOT API requests

    Methods
    -------
    __get_access_token()
        Gets access token from POST request to ERCOT API.
    __get_data()
        Gets raw data ERCOT API and combines all pages of data into raw DataFrame to be formatted by caller function.
    get_connection_status()
        Test if Object is connected to ERCOT API.
    get_dam_lmp()
        Get DAM LMP.
    get_dam_spp()
        Get DAM SPP.
    get_dam_shadow_prices()
        Get DAM Shadow Prices.
    get_dam_system_lambda()
        Get DAM System Lambda.
    get_dam_tot_as()
        Get ERCOT DAM total ancillary services offered by category.
    get_rtm_sced_shadow()
        Get RTM Shadow Prices.
    get_rtm_sced_lambda()
        Get RTM System Lambda.
    get_rtm_spp_lz()
        Get RTM SPP by LZ.
    get_rtm_spp_hub()
        Get RTM SPP by Hub.
    get_rtm_spp_rn()
        Get RTM SPP by RN.
    get_7day_load_studyarea()
        Get 7-day load forecast by study area.
    get_7day_load_weatherzone()
        Get 7-day load forecast by weatherzone.
    get_solar_production_geo()
        Get solar production by geographical zone.
    get_wind_production_geo()
        Get wind production by geographical zone.
    get_solar_production_lz()
        Get solar production by load zone.
    get_wind_production_lz()
        Get wind production by load zone.
    set_username()
        Set new username, option to reset API connection.
    set_password()
        Set new password, option to reset API connection.
    set_api_key()
        Set API key, option to reset API connection.
    set_api_connection()
        Set new API connection.
    """

    def __init__(self, username:str, password:str, api_key:str) -> None:
        """
        Parameters
        ----------
        username: str
            Username for ERCOT API account.
        password: str
            Password for ERCOT API account.
        api_key: str
            API key that can be obtained from ERCOT API account.
        """

        self.__username:str = username
        self.__password:str = password
        self.__api_key:str = api_key
        self.__access_token: str = self.__get_access_token()
        self.__authentication_header:dict = {"authorization": "Bearer " + self.__access_token, "Ocp-Apim-Subscription-Key": self.__api_key}
        self.__connection_cutoff:dt.datetime = dt.datetime.now()

        # Test API Connection
        self.get_connection_status()
        
        return None
    
    def __get_access_token(self) -> str:
        """
        Summary
        -------
        Gets access token from POST request to ERCOT API.
        """

        # Link segments for POST request
        token_link_start:str = "https://ercotb2c.b2clogin.com/ercotb2c.onmicrosoft.com/B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"
        token_link_end:str = "&grant_type=password&scope=openid+fec253ea-0d06-4272-a5e6-b478baeecd70+offline_access&client_id=fec253ea-0d06-4272-a5e6-b478baeecd70&response_type=id_token"
        
        # Make POST request, return ID token
        return post(token_link_start + 
                    "?username=" + self.__username + 
                    "&password=" + self.__password + 
                    token_link_end, verify=False).json()["id_token"]

    def __get_data(self, link:str, params:dict, raw_data:pd.DataFrame = pd.DataFrame(), current_page:int = 1, num_pages:int = 0) -> pd.DataFrame:
        """
        Summary
        -------
        Gets raw data ERCOT API and combines all pages of data into raw DataFrame to be formatted by caller function.

        Parameters
        ----------
        link:str
            Base link to build request.
        params:dict
            Dictionary of parameters and associated value to build request link.
        raw_data:pd.DataFrame
            Raw DataFrame that saves DataFrame during recursion.
        current_page:int
            Current page in pages for request.
        num_pages:int
            Number of pages to request data.
        """

        # Check if still connected to API, if not reset connection
        if dt.datetime.now() >= self.__connection_cutoff:
            self.set_api_connection()

        # Base link to add parameters and associated values to
        rq_link:str = link + "?"
        
        # Build link with parameters
        for param in params.keys():
            if params[param] != None:
                rq_link = rq_link + param + "=" + params[param] + "&"
        
        # If num_pages is zero, then total pages function has not been completed
        num_pages:int = num_pages
        if num_pages == 0:
            # Make inital request to gather how many pages of data need to be requested, store pages
            try:
                initial_request:dict = get(rq_link[:-1], headers=self.__authentication_header, verify=False).json()
                num_pages = initial_request["_meta"]["totalPages"]
            except KeyError:
                return self.__get_data(link, params)

    
        # Create DataFrame with to hold data from pages requested from ERCOT API, store first page in DataFrame
        raw_data:pd.DataFrame = raw_data

        # Loop through all pages left to request data from
        for page in range(current_page, num_pages + 1):
            try:
                # Print statment to let user know where program is in processing
                print("> Requesting Page (" + str(page) + "/" + str(num_pages) + ")")
                raw_data = pd.concat([pd.DataFrame(get(rq_link + "page=" + str(page), headers=self.__authentication_header, verify=False).json()["data"]), raw_data])
            except KeyError:
                return self.__get_data(link, params, raw_data, page, num_pages)
            time.sleep(.5)

        # Print statment to let user know where program is in processing
        print("> Formatting Data")

        # Return raw DataFrame to calling function for formatting
        return raw_data

    def get_connection_status(self) -> int:
        """
        Summary
        -------
        Test if Object is connected to ERCOT API.
        """
        
        # Create a sample request
        test_response: Response = get("https://api.ercot.com/api/public-reports/np4-190-cd/dam_stlmnt_pnt_prices?deliveryDateFrom=2024-01-01&deliveryDateTo=2024-01-01&settlementPoint=HB_WEST",
                                      headers=self.__authentication_header, 
                                      verify=False)
        # If response code is 200, connection has been made
        if test_response.status_code == 200:
            print("> Connected To API\n")
            self.__connection_cutoff = dt.datetime.now() + dt.timedelta(minutes=50)
            return test_response.status_code
        # If response code is not 200, connection has not been made
        else:
            print("> Not Connected To API: Check Username/Password/API Key\n")
            return test_response.status_code

    def get_dam_lmp(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, busName:str = None, size:int = 200000):
        """
        Summary
        -------
        Get DAM LMP.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        busName:str
            Bus name to filter request by.
            No argument returns all buses, only one bus can be requested at a time if specified.
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np4-183-cd/dam_hourly_lmp",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "busName": busName,
                                                "size" : str(size)})

        raw_data = raw_data.drop(4, axis=1)
        raw_data[0] = pd.to_datetime(raw_data[0], format="mixed")
        raw_data[1] = [int(x[:-3]) for x in raw_data[1]]
        raw_data[3] = raw_data[3].astype(float)

        for hour in range(1,25):
            raw_data[1] = raw_data[1].replace(hour, hour - 1)

        raw_data = raw_data.rename(columns={0:"Date", 1:"HE", 2:"Unit", 3:"LMP"})

        date_range = pd.date_range(dt.datetime.strptime(deliveryDateFrom, "%Y-%m-%d"),
                                   dt.datetime.strptime(deliveryDateTo, "%Y-%m-%d"),
                                   freq="D")
        
        # Create collection DataFrame to store formatted response code data
        formatted_df:pd.DataFrame = pd.DataFrame(index=pd.MultiIndex.from_product(iterables=[date_range, [x for x in range(24)]]))
        formatted_df = formatted_df.reset_index()
        formatted_df = formatted_df.rename(columns={"level_0": "Date", "level_1": "HE"})

        for unit in raw_data["Unit"].unique():
            temp_df:pd.DataFrame = raw_data.loc[raw_data["Unit"] == unit]
            temp_df = temp_df.rename(columns={"LMP":temp_df["Unit"].iloc[0]})
            temp_df = temp_df.drop("Unit", axis=1)
            temp_df["New Index"] = [x for x in range(len(temp_df))]
            temp_df.set_index("New Index", inplace=True)

            # Look for dates missing in current DataFrame within the date range of the collection DataFrame
            for date in date_range:
                if date in temp_df["Date"].values:
                    if len(temp_df["HE"].loc[temp_df["Date"] == date].to_list()) != 24:
                        for hour in range(24):
                            if len(temp_df["HE"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour)].to_list()) == 0:
                                temp_df.loc[len(temp_df)] = {"Date":date, "HE":hour, unit:nan}
                else:
                    for hour in range(24):
                        temp_df.loc[len(temp_df)] = {"Date":date, "HE":hour, unit:nan}
            
            # Drop duplicate time series
            temp_df.drop_duplicates(subset=["Date", "HE"], keep="first", inplace=True)

            # Sort values to line up with index in collection DataFrame
            temp_df.sort_values(["Date", "HE"], inplace=True)
            
            # Set new index so indexes match in concatenation
            temp_df["New Index"] = [x for x in range(len(temp_df))]
            
            temp_df.set_index("New Index", inplace=True)
            
            formatted_df = pd.merge(formatted_df, temp_df, how="left", left_on=["Date", "HE"], right_on=["Date", "HE"])
        
        formatted_df.set_index(["Date", "HE"], inplace=True)
         
        return formatted_df

    def get_dam_spp(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, settlementPoint:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get DAM SPP.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        settlementPoint:str
            Settlement point to filter request by.
            No argument returns all settlement points, only one settlement point can be requested at a time if specified.
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np4-190-cd/dam_stlmnt_pnt_prices",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "settlementPoint": settlementPoint,
                                                "size" : str(size)})
        
        raw_data = raw_data.drop(4, axis=1)
        raw_data[0] = pd.to_datetime(raw_data[0], format="mixed")
        raw_data[1] = [int(x[:-3]) for x in raw_data[1]]
        raw_data[3] = raw_data[3].astype(float)

        for hour in range(1,25):
            raw_data[1] = raw_data[1].replace(hour, hour - 1)

        raw_data = raw_data.rename(columns={0:"Date", 1:"HE", 2:"Unit", 3:"SPP"})

        date_range = pd.date_range(dt.datetime.strptime(deliveryDateFrom, "%Y-%m-%d"),
                                   dt.datetime.strptime(deliveryDateTo, "%Y-%m-%d"),
                                   freq="D")
        
        # Create collection DataFrame to store formatted response code data
        formatted_df:pd.DataFrame = pd.DataFrame(index=pd.MultiIndex.from_product(iterables=[date_range, [x for x in range(24)]]))
        formatted_df = formatted_df.reset_index()
        formatted_df = formatted_df.rename(columns={"level_0": "Date", "level_1": "HE"})

        for unit in raw_data["Unit"].unique():
            temp_df:pd.DataFrame = raw_data.loc[raw_data["Unit"] == unit]
            temp_df = temp_df.rename(columns={"SPP":temp_df["Unit"].iloc[0]})
            temp_df = temp_df.drop("Unit", axis=1)
            temp_df["New Index"] = [x for x in range(len(temp_df))]
            temp_df.set_index("New Index", inplace=True)

            # Look for dates missing in current DataFrame within the date range of the collection DataFrame
            for date in date_range:
                if date in temp_df["Date"].values:
                    if len(temp_df["HE"].loc[temp_df["Date"] == date].to_list()) != 24:
                        for hour in range(24):
                            if len(temp_df["HE"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour)].to_list()) == 0:
                                temp_df.loc[len(temp_df)] = {"Date":date, "HE":hour, unit:nan}
                else:
                    for hour in range(24):
                        temp_df.loc[len(temp_df)] = {"Date":date, "HE":hour, unit:nan}
            
            # Drop duplicate time series
            temp_df.drop_duplicates(subset=["Date", "HE"], keep="first", inplace=True)

            # Sort values to line up with index in collection DataFrame
            temp_df.sort_values(["Date", "HE"], inplace=True)
            
            # Set new index so indexes match in concatenation
            temp_df["New Index"] = [x for x in range(len(temp_df))]
            
            temp_df.set_index("New Index", inplace=True)
            
            formatted_df = pd.merge(formatted_df, temp_df, how="left", left_on=["Date", "HE"], right_on=["Date", "HE"])
        
        formatted_df.set_index(["Date", "HE"], inplace=True)
         
        return formatted_df
 
    def get_dam_shadow_prices(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, constraintName:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get DAM Shadow Prices.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        constraintName:str
            Constraint name to filter request by.
            No argument returns all constraints, only one constraint can be requested at a time if specified.
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np4-191-cd/dam_shadow_prices",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "constraintName": constraintName,
                                                "size" : str(size)})

        raw_data[0] = pd.to_datetime(raw_data[0], format="mixed")
        raw_data[1] = [int(x[:-3]) for x in raw_data[1]]

        for hour in range(1, 25):
            raw_data[1] = raw_data[1].replace(hour, hour - 1)

        raw_data[[5,6,7,8]] = raw_data[[5,6,7,8]].astype(float)
        raw_data[[11,12]] = raw_data[[11,12]].astype(int).astype(float)
        raw_data.drop([13,14], axis=1, inplace=True)

        raw_data.rename(columns={0:"Date", 1:"HE", 2:"Constraint ID", 3:"Constraint Name",
                                 4:"Contingency Name", 5:"Constraint Limit", 6:"Constraint Flow",
                                 7:"MW Violation Amt.", 8:"Shadow Price", 9:"Source Station",
                                 10:"Sink Station", 11:"Source kV", 12:"Sink kV"},
                        inplace=True)
        
        raw_data = raw_data.sort_values(["Date", "HE"])

        raw_data.set_index(["Date", "HE"], inplace=True)

        return raw_data
                        
    def get_dam_system_lambda(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get DAM System Lambda.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np4-523-cd/dam_system_lambda",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "size" : str(size)})
        
        raw_data[0] = pd.to_datetime(raw_data[0], format="mixed")

        raw_data[1] = [int(x[:-3]) for x in raw_data[1]]

        for hour in range(1, 25):
            raw_data[1] = raw_data[1].replace(hour, hour - 1)

        raw_data[2] = raw_data[2].astype(float)

        raw_data.drop(3, axis=1, inplace=True)

        raw_data = raw_data.rename(columns={0:"Date", 1:"HE", 2:"System Lambda"})

        raw_data = raw_data.sort_values(["Date", "HE"])

        raw_data.set_index(["Date", "HE"], inplace=True)

        return raw_data

    def get_dam_tot_as(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, size:int = 200000):
        """
        Summary
        -------
        Get ERCOT DAM total ancillary services offered by category.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np4-179-cd/total_as_service_offers",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "size" : str(size)})
        
        raw_data = raw_data.drop(10, axis=1)
        raw_data[0] = pd.to_datetime(raw_data[0], format="mixed")
        raw_data[1] = [int(x[:-3]) for x in raw_data[1]]

        for hour in range(1, 25):
            raw_data[1] = raw_data[1].replace(hour, hour - 1)

        for column in raw_data.columns[2:]:
            raw_data[column] = raw_data[column].astype(float)
        
        raw_data = raw_data.rename(columns={0:"Date", 1:"HE", 2:"REGDN", 3:"REGUP",
                                            4:"RRSPFR", 5:"RRSFFR", 6:"RRSUFR", 7:"ECRSSD",
                                            8:"ECRSMD", 9:"NSPIN"})
        
        return raw_data

    def get_rtm_sced_shadow(self, SCEDTimestampFrom:str = None, SCEDTimestampTo:str = None, constraintName:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get RTM Shadow Prices.

        Parameters
        ----------
        SCEDTimestampFrom:str
            SCED timestamp start, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        SCEDTimestampTo:str
            SCED timestamp end, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        constraintName:str
            Constraint name to filter request by.
            No argument returns all constraints, only one constraint can be requested at a time if specified.
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np6-86-cd/shdw_prices_bnd_trns_const",
                                               {"SCEDTimestampFrom": SCEDTimestampFrom,
                                                "SCEDTimestampTo" : SCEDTimestampTo,
                                                "constraintName" : constraintName,
                                                "size" : str(size)})
        
        raw_data[0] = [str(x) for x in pd.to_datetime(raw_data[0], format="mixed")]
        raw_data["Date"] = [x[:10] for x in raw_data[0]]
        raw_data["Date"] = pd.to_datetime(raw_data["Date"], format="mixed")
        raw_data["HE"] = [int(x[-8:-6]) for x in raw_data[0]]
        raw_data["Interval"] = [int(x[-5:-3]) for x in raw_data[0]]

        raw_data[2] = raw_data[2].astype(float)
        raw_data[[5,6,7,8,9]] = raw_data[[5,6,7,8,9]].astype(float)
        raw_data[[12,13]] = raw_data[[12,13]].astype(int).astype(float)

        raw_data.drop([0,1], axis=1, inplace=True)

        raw_data.rename(columns={2:"Constraint ID", 3:"Constraint Name", 4:"Contingency Name",
                                 5:"Shadow Price", 6:"Max Shadow Price", 7:"Constraint Limit",
                                 8:"Constraint Flow", 9:"MW Violatio Amt.", 10:"Source Station",
                                 11:"Sink Sattion", 12:"Source kV", 13:"Sink kV",
                                 14:"CCT Status"},
                        inplace=True)
        
        raw_data = raw_data.sort_values(["Date", "HE", "Interval"])

        raw_data.set_index(["Date", "HE", "Interval"], inplace=True)
        
        return raw_data
        
    def get_rtm_sced_lambda(self, SCEDTimestampFrom:str = None, SCEDTimestampTo:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get RTM System Lambda.

        Parameters
        ----------
        SCEDTimestampFrom:str
            SCED timestamp start, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        SCEDTimestampTo:str
            SCED timestamp end, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np6-322-cd/sced_system_lambda",
                                               {"SCEDTimestampFrom": SCEDTimestampFrom,
                                                "SCEDTimestampTo" : SCEDTimestampTo,
                                                "size" : str(size)})
        
        raw_data[0] = [str(x) for x in pd.to_datetime(raw_data[0], format="mixed")]
        raw_data["Date"] = [x[:10] for x in raw_data[0]]
        raw_data["Date"] = pd.to_datetime(raw_data["Date"], format="mixed")
        raw_data["HE"] = [int(x[-8:-6]) for x in raw_data[0]]
        raw_data["Interval"] = [int(x[-5:-3]) for x in raw_data[0]]

        raw_data[2] = raw_data[2].astype(float)

        raw_data.drop([0,1], axis=1, inplace=True)

        raw_data = raw_data.rename(columns={2:"System Lambda"})

        raw_data = raw_data.sort_values(["Date","HE","Interval"])

        raw_data.set_index(["Date", "HE", "Interval"], inplace=True)

        return raw_data
        
    def get_rtm_spp_lz(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, settlementPoint:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get RTM SPP by LZ.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        settlementPoint:str
            Settlement point to filter request by.
            No argument returns all settlement points, only one settlement point can be requested at a time if specified.
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np6-905-cd/spp_node_zone_hub",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "settlementPoint": settlementPoint,
                                                "size" : str(size)})
        
        raw_data = raw_data.loc[raw_data[4] == "LZ"]
        raw_data[0] = pd.to_datetime(raw_data[0], format="mixed")
        raw_data[[1,2]] = raw_data[[1,2]].astype(int)

        for hour in range(1, 25):
            raw_data[1] = raw_data[1].replace(hour, hour - 1)

        raw_data[2] = raw_data[2].replace(1,0)
        raw_data[2] = raw_data[2].replace(2,15)
        raw_data[2] = raw_data[2].replace(3,30)
        raw_data[2] = raw_data[2].replace(4,45)

        raw_data[5] = raw_data[5].astype(float)

        raw_data.drop([4,6], axis=1, inplace=True)

        raw_data = raw_data.rename(columns={0:"Date", 1:"HE", 2:"Interval",
                                            3:"Unit", 5:"SPP"})
        
        date_range = pd.date_range(dt.datetime.strptime(deliveryDateFrom, "%Y-%m-%d"),
                                   dt.datetime.strptime(deliveryDateTo, "%Y-%m-%d"),
                                   freq="D")
        
        # Create collection DataFrame to store formatted response code data
        formatted_df = pd.DataFrame(index=pd.MultiIndex.from_product(iterables=[date_range, [x for x in range(24)], [0, 15, 30, 45]]))
        formatted_df.reset_index(inplace=True)
        formatted_df.rename(columns={"level_0": "Date", "level_1": "HE", "level_2": "Interval"}, inplace=True)
        
        for unit in raw_data["Unit"].unique():
            temp_df:pd.DataFrame = raw_data.loc[raw_data["Unit"] == unit]
            temp_df = temp_df.rename(columns={"SPP":temp_df["Unit"].iloc[0]})
            temp_df = temp_df.drop("Unit", axis=1)
            temp_df["New Index"] = [x for x in range(len(temp_df))]
            temp_df.set_index("New Index", inplace=True)

            # Look for dates missing in current DataFrame within the date range of the collection DataFrame
            for date in date_range:
                if date in temp_df["Date"].values:
                    if len(temp_df["HE"].loc[temp_df["Date"] == date].to_list()) == 24:
                        for hour in range(24):
                            if len(temp_df["Interval"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour)].to_list()) != 4:
                                for interval in [0, 15, 30, 45]:
                                    if len(temp_df["Interval"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour) & (temp_df["Interval"] == interval)].to_list()) == 0:
                                        temp_df.loc[len(temp_df)] = {"Date":date, "HE": hour, "Interval": interval, unit:nan}
                    else:
                        for hour in range(24):
                            if len(temp_df["HE"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour)].to_list()) == 0:
                                for interval in [0, 15, 30, 45]:
                                    temp_df.loc[len(temp_df)] = {"Date":date, "HE":hour, "Interval":interval, unit:nan}
                            elif len(temp_df["Interval"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour)].to_list()) != 4:
                                for interval in [0, 15, 30, 45]:
                                    if len(temp_df["Interval"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour) & (temp_df["Interval"] == interval)].to_list()) == 0:
                                        temp_df.loc[len(temp_df)] = {"Date":date, "HE":hour, "Interval":interval, unit:nan}
                else:
                    for hour in range(24):
                        for interval in [0, 15, 30, 45]:
                            temp_df.loc[len(temp_df)] = {"Date":date, "HE": hour, "Interval": interval, unit:nan}
            
            # Drop duplicate time series
            temp_df.drop_duplicates(subset=["Date", "HE", "Interval"], keep="first", inplace=True)

            # Sort values to line up with index in collection DataFrame
            temp_df.sort_values(["Date", "HE", "Interval"], inplace=True)
            
            # Set new index so indexes match in concatenation
            temp_df["New Index"] = [x for x in range(len(temp_df))]
            
            temp_df.set_index("New Index", inplace=True)
            
            formatted_df = pd.merge(formatted_df, temp_df, how="left", left_on=["Date", "HE", "Interval"], right_on=["Date", "HE", "Interval"])
        
        formatted_df.set_index(["Date", "HE", "Interval"], inplace=True)
        
        return formatted_df

    def get_rtm_spp_hub(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, settlementPoint:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get RTM SPP by Hub.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        settlementPoint:str
            Settlement point to filter request by.
            No argument returns all settlement points, only one settlement point can be requested at a time if specified.
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np6-905-cd/spp_node_zone_hub",
                                                {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "settlementPoint": settlementPoint,
                                                "size" : str(size)})
            
        raw_data = raw_data.loc[raw_data[4] == "HU"]
        raw_data[0] = pd.to_datetime(raw_data[0], format="mixed")
        raw_data[[1,2]] = raw_data[[1,2]].astype(int)

        for hour in range(1, 25):
            raw_data[1] = raw_data[1].replace(hour, hour - 1)

        raw_data[2] = raw_data[2].replace(1,0)
        raw_data[2] = raw_data[2].replace(2,15)
        raw_data[2] = raw_data[2].replace(3,30)
        raw_data[2] = raw_data[2].replace(4,45)

        raw_data[5] = raw_data[5].astype(float)

        raw_data.drop([4,6], axis=1, inplace=True)

        raw_data = raw_data.rename(columns={0:"Date", 1:"HE", 2:"Interval",
                                                3:"Unit", 5:"SPP"})
            
        date_range = pd.date_range(dt.datetime.strptime(deliveryDateFrom, "%Y-%m-%d"),
                                   dt.datetime.strptime(deliveryDateTo, "%Y-%m-%d"),
                                   freq="D")
            
        # Create collection DataFrame to store formatted response code data
        formatted_df = pd.DataFrame(index=pd.MultiIndex.from_product(iterables=[date_range, [x for x in range(24)], [0, 15, 30, 45]]))
        formatted_df.reset_index(inplace=True)
        formatted_df.rename(columns={"level_0": "Date", "level_1": "HE", "level_2": "Interval"}, inplace=True)
            
        for unit in raw_data["Unit"].unique():
            temp_df:pd.DataFrame = raw_data.loc[raw_data["Unit"] == unit]
            temp_df = temp_df.rename(columns={"SPP":temp_df["Unit"].iloc[0]})
            temp_df = temp_df.drop("Unit", axis=1)
            temp_df["New Index"] = [x for x in range(len(temp_df))]
            temp_df.set_index("New Index", inplace=True)

            # Look for dates missing in current DataFrame within the date range of the collection DataFrame
            for date in date_range:
                if date in temp_df["Date"].values:
                    if len(temp_df["HE"].loc[temp_df["Date"] == date].to_list()) == 24:
                        for hour in range(24):
                            if len(temp_df["Interval"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour)].to_list()) != 4:
                                for interval in [0, 15, 30, 45]:
                                    if len(temp_df["Interval"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour) & (temp_df["Interval"] == interval)].to_list()) == 0:
                                        temp_df.loc[len(temp_df)] = {"Date":date, "HE": hour, "Interval": interval, unit:nan}
                    else:
                        for hour in range(24):
                            if len(temp_df["HE"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour)].to_list()) == 0:
                                for interval in [0, 15, 30, 45]:
                                    temp_df.loc[len(temp_df)] = {"Date":date, "HE":hour, "Interval":interval, unit:nan}
                            elif len(temp_df["Interval"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour)].to_list()) != 4:
                                for interval in [0, 15, 30, 45]:
                                    if len(temp_df["Interval"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour) & (temp_df["Interval"] == interval)].to_list()) == 0:
                                        temp_df.loc[len(temp_df)] = {"Date":date, "HE":hour, "Interval":interval, unit:nan}
                else:
                    for hour in range(24):
                        for interval in [0, 15, 30, 45]:
                            temp_df.loc[len(temp_df)] = {"Date":date, "HE": hour, "Interval": interval, unit:nan}
                
            # Drop duplicate time series
            temp_df.drop_duplicates(subset=["Date", "HE", "Interval"], keep="first", inplace=True)

            # Sort values to line up with index in collection DataFrame
            temp_df.sort_values(["Date", "HE", "Interval"], inplace=True)
                
            # Set new index so indexes match in concatenation
            temp_df["New Index"] = [x for x in range(len(temp_df))]
                
            temp_df.set_index("New Index", inplace=True)
                
            formatted_df = pd.merge(formatted_df, temp_df, how="left", left_on=["Date", "HE", "Interval"], right_on=["Date", "HE", "Interval"])
            
        formatted_df["HB_HUBAVG"] = formatted_df[formatted_df.columns[3:]].mean(axis=1)

        formatted_df.set_index(["Date", "HE", "Interval"], inplace=True)
            
        return formatted_df

    def get_rtm_spp_rn(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, settlementPoint:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get RTM SPP by RN.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        settlementPoint:str
            Settlement point to filter request by.
            No argument returns all settlement points, only one settlement point can be requested at a time if specified.
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np6-905-cd/spp_node_zone_hub",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "settlementPoint": settlementPoint,
                                                "size" : str(size)})
            
        raw_data = raw_data.loc[raw_data[4] == "RN"]
        raw_data[0] = pd.to_datetime(raw_data[0], format="mixed")
        raw_data[[1,2]] = raw_data[[1,2]].astype(int)

        for hour in range(1, 25):
            raw_data[1] = raw_data[1].replace(hour, hour - 1)

        raw_data[2] = raw_data[2].replace(1,0)
        raw_data[2] = raw_data[2].replace(2,15)
        raw_data[2] = raw_data[2].replace(3,30)
        raw_data[2] = raw_data[2].replace(4,45)

        raw_data[5] = raw_data[5].astype(float)

        raw_data.drop([4,6], axis=1, inplace=True)

        raw_data = raw_data.rename(columns={0:"Date", 1:"HE", 2:"Interval",
                                                3:"Unit", 5:"SPP"})
            
        date_range = pd.date_range(dt.datetime.strptime(deliveryDateFrom, "%Y-%m-%d"),
                                   dt.datetime.strptime(deliveryDateTo, "%Y-%m-%d"),
                                   freq="D")
            
        # Create collection DataFrame to store formatted response code data
        formatted_df = pd.DataFrame(index=pd.MultiIndex.from_product(iterables=[date_range, [x for x in range(24)], [0, 15, 30, 45]]))
        formatted_df.reset_index(inplace=True)
        formatted_df.rename(columns={"level_0": "Date", "level_1": "HE", "level_2": "Interval"}, inplace=True)

        for unit in raw_data["Unit"].unique():
            temp_df:pd.DataFrame = raw_data.loc[raw_data["Unit"] == unit]
            temp_df = temp_df.rename(columns={"SPP":temp_df["Unit"].iloc[0]})
            temp_df = temp_df.drop("Unit", axis=1)
            temp_df["New Index"] = [x for x in range(len(temp_df))]
            temp_df.set_index("New Index", inplace=True)

            # Look for dates missing in current DataFrame within the date range of the collection DataFrame
            for date in date_range:
                if date in temp_df["Date"].values:
                    if len(temp_df["HE"].loc[temp_df["Date"] == date].to_list()) == 24:
                        for hour in range(24):
                            if len(temp_df["Interval"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour)].to_list()) != 4:
                                for interval in [0, 15, 30, 45]:
                                    if len(temp_df["Interval"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour) & (temp_df["Interval"] == interval)].to_list()) == 0:
                                        temp_df.loc[len(temp_df)] = {"Date":date, "HE": hour, "Interval": interval, unit:nan}
                    else:
                        for hour in range(24):
                            if len(temp_df["HE"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour)].to_list()) == 0:
                                for interval in [0, 15, 30, 45]:
                                    temp_df.loc[len(temp_df)] = {"Date":date, "HE":hour, "Interval":interval, unit:nan}
                            elif len(temp_df["Interval"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour)].to_list()) != 4:
                                for interval in [0, 15, 30, 45]:
                                    if len(temp_df["Interval"].loc[(temp_df["Date"] == date) & (temp_df["HE"] == hour) & (temp_df["Interval"] == interval)].to_list()) == 0:
                                        temp_df.loc[len(temp_df)] = {"Date":date, "HE":hour, "Interval":interval, unit:nan}
                else:
                    for hour in range(24):
                        for interval in [0, 15, 30, 45]:
                            temp_df.loc[len(temp_df)] = {"Date":date, "HE": hour, "Interval": interval, unit:nan}
                
            # Drop duplicate time series
            temp_df.drop_duplicates(subset=["Date", "HE", "Interval"], keep="first", inplace=True)

            # Sort values to line up with index in collection DataFrame
            temp_df.sort_values(["Date", "HE", "Interval"], inplace=True)
                
            # Set new index so indexes match in concatenation
            temp_df["New Index"] = [x for x in range(len(temp_df))]
                
            temp_df.set_index("New Index", inplace=True)
                
            formatted_df = pd.merge(formatted_df, temp_df, how="left", left_on=["Date", "HE", "Interval"], right_on=["Date", "HE", "Interval"])
            
        formatted_df.set_index(["Date", "HE", "Interval"], inplace=True)
            
        return formatted_df

    def get_7day_load_studyarea(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, postedDatetimeFrom:str = None, postedDatetimeTo:str = None, model:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get 7-day load forecast by study area.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        postedDatetimeFrom:str
            Posted timestamp start, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        postedDatetimeTo:str
            Posted timestamp end, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        model:str
            Model to filter search by.
        size:int
            Number of records per page.
        """
        
        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np3-566-cd/lf_by_model_study_area",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "postedDatetimeFrom": postedDatetimeFrom,
                                                "postedDatetimeTo" : postedDatetimeTo,
                                                "model" : model,
                                                "size" : str(size)})
        
        return raw_data
        
    def get_7day_load_weatherzone(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, postedDatetimeFrom:str = None, postedDatetimeTo:str = None, model:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get 7-day load forecast by weatherzone.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        postedDatetimeFrom:str
            Posted timestamp start, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        postedDatetimeTo:str
            Posted timestamp end, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        model:str
            Model to filter search by.
        size:int
            Number of records per page.
        """
        
        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np3-565-cd/lf_by_model_weather_zone",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "postedDatetimeFrom": postedDatetimeFrom,
                                                "postedDatetimeTo" : postedDatetimeTo,
                                                "model" : model,
                                                "size" : str(size)})
        
        return raw_data
        
    def get_solar_production_geo(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, postedDatetimeFrom:str = None, postedDatetimeTo:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get solar production by geographical zone.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        postedDatetimeFrom:str
            Posted timestamp start, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        postedDatetimeTo:str
            Posted timestamp end, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np4-745-cd/spp_hrly_actual_fcast_geo",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "postedDatetimeFrom": postedDatetimeFrom,
                                                "postedDatetimeTo" : postedDatetimeTo,
                                                "size" : str(size)})
        
        return raw_data
        
    def get_wind_production_geo(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, postedDatetimeFrom:str = None, postedDatetimeTo:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get wind production by geographical zone.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        postedDatetimeFrom:str
            Posted timestamp start, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        postedDatetimeTo:str
            Posted timestamp end, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np4-742-cd/wpp_hrly_actual_fcast_geo",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "postedDatetimeFrom": postedDatetimeFrom,
                                                "postedDatetimeTo" : postedDatetimeTo,
                                                "size" : str(size)})
        
        return raw_data
        
    def get_solar_production_lz(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, postedDatetimeFrom:str = None, postedDatetimeTo:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get solar production by load zone.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        postedDatetimeFrom:str
            Posted timestamp start, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        postedDatetimeTo:str
            Posted timestamp end, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        size:int
            Number of records per page.
        """

        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np4-737-cd/spp_hrly_avrg_actl_fcast",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "postedDatetimeFrom": postedDatetimeFrom,
                                                "postedDatetimeTo" : postedDatetimeTo,
                                                "size" : str(size)})
        
        return raw_data
        
    def get_wind_production_lz(self, deliveryDateFrom:str = None, deliveryDateTo:str = None, postedDatetimeFrom:str = None, postedDatetimeTo:str = None, size:int = 200000) -> pd.DataFrame:
        """
        Summary
        -------
        Get wind production by load zone.

        Parameters
        ----------
        deliveryDateFrom:str
            Delivery date start, inclusive
            Format: "YYYY-mm-dd"
        deliveryDateTo:str
            Delivery date end, inclusive
            Format: "YYYY-mm-dd"
        postedDatetimeFrom:str
            Posted timestamp start, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        postedDatetimeTo:str
            Posted timestamp end, inclusive
            Format: "YYYY-mm-ddTHH:MM:SS"
        size:int
            Number of records per page.
        """
        
        raw_data:pd.DataFrame = self.__get_data("https://api.ercot.com/api/public-reports/np4-732-cd/wpp_hrly_avrg_actl_fcast",
                                               {"deliveryDateFrom": deliveryDateFrom,
                                                "deliveryDateTo" : deliveryDateTo,
                                                "postedDatetimeFrom": postedDatetimeFrom,
                                                "postedDatetimeTo" : postedDatetimeTo,
                                                "size" : str(size)})
        
        return raw_data
         
    def set_username(self, username:str, reset_connection:bool = False) -> str:
        """
        Summary
        -------
        Set new username, option to reset API connection.

        Parameters
        ----------
        username:str
            Username for the ERCOT API connection.
        reset_connection:bool
            Reset connection to ERCOT API.
        """

        self.__username = username

        if reset_connection:
            self.set_api_connection()
        
        return self.__username

    def set_password(self, password:str, reset_connection:bool = False) -> str:
        """
        Summary
        -------
        Set new password, option to reset API connection.

        Parameters
        ----------
        password:str
            Password for the ERCOT API connection.
        reset_connection:bool
            Reset connection to ERCOT API.
        """

        self.__password = password

        if reset_connection:
            self.set_api_connection()
        
        return self.__password

    def set_api_key(self, api_key:str, reset_connection:bool = False) -> bool:
        """
        Summary
        -------
        Set API key, option to reset API connection.

        Parameters
        ----------
        api_key:str
            API key for the ERCOT API connection.
        reset_connection:bool
            Reset connection to ERCOT API.
        """

        self.__api_key = api_key

        if reset_connection:
            self.set_api_connection()

        return self.__api_key

    def set_api_connection(self) -> bool:
        """
        Summary
        -------
        Set new API connection.
        """
        
        # Get new access token and create new authentication header
        self.__access_token = self.__get_access_token()
        self.__authentication_header = {"authorization": "Bearer " + self.__access_token, "Ocp-Apim-Subscription-Key": self.__api_key}
        
        # Test API Connection
        self.get_connection_status()

        return True
