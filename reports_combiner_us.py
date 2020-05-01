import pandas as pd
import os
from sqlalchemy import create_engine,exc

directory_cases_us = r'COVID-19\csse_covid_19_data\csse_covid_19_time_series\time_series_covid19_confirmed_US.csv'
directory_deaths_us = r'COVID-19\csse_covid_19_data\csse_covid_19_time_series\time_series_covid19_deaths_US.csv'

df_cases_us = pd.read_csv(directory_cases_us)
df_deaths_us = pd.read_csv(directory_deaths_us)

df_cases_us = df_cases_us.rename(columns={'Country_Region':'country_region','Province_State':'province_state','Admin2':'county'})
df_deaths_us = df_deaths_us.rename(columns={'Country_Region':'country_region','Province_State':'province_state','Admin2':'county','Population':'population'})

columns_drop = ['UID','iso2','iso3','code3','Lat','Long_','Combined_Key']
df_cases_us = df_cases_us.drop(columns=columns_drop,axis=1)
df_deaths_us = df_deaths_us.drop(columns=columns_drop,axis=1)

processed_dir = 'processed_data'
csv_filename = 'raw_data_us.csv'
db_filename = 'data_us.db'
engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')

def reports_combiner(date):
    check_if_data_already_added(date)
    date_1 = pd.to_datetime(date).strftime('%m/%d/%y').lstrip("0").replace("/0", "/")
    store_data(_us_county(date_1))
    
def check_if_data_already_added(date):
    try:
        df = pd.read_sql(f"SELECT * FROM raw_data WHERE Date == '{date}'",con=engine)
    except exc.OperationalError:
        return 1
    if not df.empty:
        raise EOFError('Data Already Exists in DB')
    else:
        return 1

def store_data(df):
    # SQLite and CSV
    df.to_sql(f'raw_data',if_exists='append',con=engine,index=True)

    if not os.path.isfile(f'{processed_dir}/{csv_filename}'):
        df.to_csv(f'{processed_dir}/{csv_filename}',index=True)
    else: # else it exists so append without writing the header
        df.to_csv(f'{processed_dir}/{csv_filename}',index=True, mode='a', header=False)

def _us_county(date_1):
    df_cases_us_for_date = df_cases_us[['FIPS','county','province_state',date_1]]
    df_cases_us_for_date = df_cases_us_for_date.rename(columns={date_1:'cases'})
    
    df_deaths_us_for_date = df_deaths_us[['FIPS','county','province_state',date_1]]
    df_deaths_us_for_date = df_deaths_us_for_date.rename(columns={date_1:'mortality'})

    df = pd.merge(df_cases_us_for_date,df_deaths_us_for_date,how='left',left_on=['FIPS','county','province_state'],right_on=['FIPS','county','province_state'])
    
    df['country_region'] = 'US'
    
    df['date'] = pd.to_datetime(date_1)
    df = df.reset_index().set_index('date')
    df = df[['FIPS','county','province_state','country_region','cases','mortality']]
    return df

if __name__ == "__main__":
    # from datetime import date,timedelta
    # latest_date = date.today()-timedelta(days=1)
    # latest_date = latest_date.strftime('%m-%d-%Y')
    # reports_combiner('04-28-2020')

    # reports_combiner('2020-04-25')

    import numpy as np
    dates = np.arange('2020-01-22','2020-04-29',dtype='datetime64[D]')
    for date in dates:
        reports_combiner(date)