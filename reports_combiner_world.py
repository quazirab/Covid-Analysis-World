import pandas as pd
import os
from sqlalchemy import create_engine,exc

directory_cases_global = r'COVID-19\csse_covid_19_data\csse_covid_19_time_series\time_series_covid19_confirmed_global.csv'
directory_deaths_global = r'COVID-19\csse_covid_19_data\csse_covid_19_time_series\time_series_covid19_deaths_global.csv'

directory_cases_us = r'COVID-19\csse_covid_19_data\csse_covid_19_time_series\time_series_covid19_confirmed_US.csv'
directory_deaths_us = r'COVID-19\csse_covid_19_data\csse_covid_19_time_series\time_series_covid19_deaths_US.csv'

df_cases_global = pd.read_csv(directory_cases_global)
df_deaths_global = pd.read_csv(directory_deaths_global)

df_cases_global = df_cases_global.rename(columns={'Country/Region':'country_region','Province/State':'province_state'})
df_deaths_global = df_deaths_global.rename(columns={'Country/Region':'country_region','Province/State':'province_state'})

df_cases_us = pd.read_csv(directory_cases_us)
df_deaths_us = pd.read_csv(directory_deaths_us)


df_cases_us = df_cases_us.rename(columns={'Country_Region':'country_region','Province_State':'province_state'})
df_deaths_us = df_deaths_us.rename(columns={'Country_Region':'country_region','Province_State':'province_state'})



processed_dir = 'processed_data'
csv_filename = 'raw_data_country.csv'
db_filename = 'data_country.db'
engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')

def reports_combiner(date):
    check_if_data_already_added(date)
    date_1 = pd.to_datetime(date).strftime('%m/%d/%y').lstrip("0").replace("/0", "/")
    store_data(_global(date_1))
    store_data(_us(date_1))
    
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

def _global(date_1):
    df_cases_global_for_date = df_cases_global[['province_state','country_region',date_1]]
    df_cases_global_for_date = df_cases_global_for_date.rename(columns={date_1:'cases'})
    df_deaths_global_for_date = df_deaths_global[['province_state','country_region',date_1]]
    df_deaths_global_for_date = df_deaths_global_for_date.rename(columns={date_1:'mortality'})
    


    df = pd.merge(df_cases_global_for_date,df_deaths_global_for_date,how='left',left_on=['country_region','province_state'],right_on=['country_region','province_state'])
    
    df_with_provinces = df[df.province_state.notnull()]

    df_with_provinces = df_with_provinces.groupby('country_region').sum().reset_index()
    df_with_provinces = df_with_provinces[df_with_provinces.country_region != 'France']
    df_with_provinces = df_with_provinces[df_with_provinces.country_region != 'Netherlands']
    df_with_provinces = df_with_provinces[df_with_provinces.country_region != 'United Kingdom']
    df = df.append(df_with_provinces)
 
    df['date'] = pd.to_datetime(date_1).date()
    df = df.set_index('date')
    return df

def _us(date_1):
    df_cases_us_for_date = df_cases_us[['province_state',date_1]]
    df_cases_us_for_date = df_cases_us_for_date.rename(columns={date_1:'cases'})
    df_cases_us_for_date = df_cases_us_for_date.groupby('province_state').sum()
    
    df_deaths_us_for_date = df_deaths_us[['province_state',date_1]]
    df_deaths_us_for_date = df_deaths_us_for_date.rename(columns={date_1:'mortality'})
    df_deaths_us_for_date = df_deaths_us_for_date.groupby('province_state').sum()

    df = pd.merge(df_cases_us_for_date,df_deaths_us_for_date,how='left',left_on='province_state',right_on='province_state')
    
    df['country_region'] = 'US'
    
    df['date'] = pd.to_datetime(date_1)
    df = df.reset_index().set_index('date')
    df = df[['province_state','country_region','cases','mortality']]
    return df

if __name__ == "__main__":
    # from datetime import date,timedelta
    # latest_date = date.today()-timedelta(days=1)
    # latest_date = latest_date.strftime('%m-%d-%Y')
    # reports_combiner('04-28-2020')

    # reports_combiner('2020-04-25')
# 1/22/20
    import numpy as np
    dates = np.arange('2020-01-22','2020-05-03',dtype='datetime64[D]')
    for date in dates:
        print(date)
        reports_combiner(date)