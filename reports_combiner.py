import pandas as pd
import os
from sqlalchemy import create_engine

directory = 'COVID-19\csse_covid_19_data\csse_covid_19_daily_reports'
processed_dir = 'processed_data'
csv_filename = 'all_country_data.csv'
db_filename = 'all_country_data.db'

engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')


df_long_lat = df = pd.read_csv(r'COVID-19\\csse_covid_19_data\\UID_ISO_FIPS_LookUp_Table.csv')
df_long_lat = df_long_lat[['Province_State','Country_Region','Lat','Long_']]
df_long_lat = df_long_lat.rename(columns={'Lat':'Latitude','Long_':'Longitude'})
df_long_lat = df_long_lat.drop_duplicates(subset=['Province_State', 'Country_Region'], keep=False)

print(df_long_lat)

def reports_combiner(date = None):
    if not date:
        # delete old database and csv
        if os.path.exists(f'{processed_dir}/{csv_filename}'):
            os.remove(f'{processed_dir}/{csv_filename}')
        if os.path.exists(f'{processed_dir}/{db_filename}'):
            os.remove(f'{processed_dir}/{db_filename}')

        for filename in os.listdir(directory):
            if filename.endswith(".csv"):
                df = _process_data(filename)
                print(df)
                exit()
                # _store_data(df)
    else:
        check_if_data_already_added()
        filename = f'{date}.csv'
        if os.path.exists(f'{directory}/{filename}'):
            df = _process_data(filename)
            _store_data(df)
        else:
            raise NameError('File Does Not Exist')

def _process_data(filename):
    df = pd.read_csv(f'{directory}/{filename}')
    
    # check _ in columns
    checked_columns = _check_columns(df.columns.tolist())
    if checked_columns:
        df = df.rename(columns=checked_columns)
    date = filename[:filename.find('.csv')]
    date = pd.to_datetime(date).date()
    df['Date'] = date
    df = df[['Date','Province_State','Country_Region','Confirmed','Deaths','Recovered']]
    df = df.replace({'Mainland China':'China'})


    # print(df)
    df = pd.merge(df,df_long_lat,how='left',left_on=['Country_Region','Province_State'],right_on=['Country_Region','Province_State'])
    df.to_csv(f'test.csv',index=False)
    return df

def _store_data(df):
    # SQLite and CSV
    df.to_sql(f'all_country',if_exists='append',con=engine)

    if not os.path.isfile(f'{processed_dir}/{csv_filename}'):
        df.to_csv(f'{processed_dir}/{csv_filename}',index=False)
    else: # else it exists so append without writing the header
        df.to_csv(f'{processed_dir}/{csv_filename}',index=False, mode='a', header=False)
    
def _check_columns(columns):
    rtnColumn = {}
    for column in columns:
        if "/" in column:
            rtnColumn[column] = column.replace('/','_')
    if rtnColumn:
        return rtnColumn
    else:
        return 0

def check_if_data_already_added(date):


    df = pd.read_sql(f"SELECT * FROM all_country WHERE Date == '{date}'",con=engine)
    if not df.empty:
        raise ImportError('Data Already Exists in DB')
    else:
        return 1

if __name__ == "__main__":
    # from datetime import date,timedelta
    # latest_date = date.today()-timedelta(days=1)
    # latest_date = latest_date.strftime('%m-%d-%Y')
    # reports_combiner('04-28-2020')

    reports_combiner()    
    # check_if_data_already_added('2020-04-20')