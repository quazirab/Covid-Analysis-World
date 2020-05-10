from sqlalchemy import create_engine
import pandas as pd
from datetime import timedelta
import numpy as np
from scipy.stats import linregress


processed_dir = 'processed_data'
db_filename = 'data_country.db'
csv_filename = 'inflection_data_world.csv'
engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')


def inflection_finder(country_region,province_state):
    if province_state: 
        df = pd.read_sql(f"SELECT date,mortality FROM raw_data where country_region == "+ '"'+ country_region +'"' + ' and ' + 'province_state ==' + '"'+ province_state +'"',con=engine,parse_dates=['date'])
    else:
        df = pd.read_sql(f"SELECT date,mortality FROM raw_data where country_region == "+ '"'+ country_region +'"' + ' and ' + 'province_state IS NULL',con=engine,parse_dates=['date'])
    
    df = df.set_index('date')
    inflection_result,inflection_date,slope_max = _find_inflection(df)

    if inflection_result:
        return inflection_result,inflection_date,slope_max
    else:
        return 0, None, None

def _find_inflection(df):
    print(df)
    df = df[df.mortality > 0]
    df = df.reset_index()
    df['day_number'] = df.index + 1
    df = df.set_index('date')
    print(df)

    indexes_to_compute = pd.date_range(start=df.index[4],end=df.index[-1])

    for index_to_compute in indexes_to_compute:
        df_for_slope = df.loc[index_to_compute-timedelta(days=4):index_to_compute]
        slope, _, _, _, _ = linregress(df_for_slope.day_number,df_for_slope.mortality)
        df.loc[index_to_compute,'slope'] = slope
    
    print(df)
    print(df.slope.idxmax().date())
    

def store_all():
    df = pd.read_sql(f"SELECT * FROM raw_data where country_region == 'Canada'",parse_dates='date',con=engine)
    df.to_csv(f'test.csv',index=True)
    df = df.groupby(['date','country_region']).sum()
    df.to_csv(f'{processed_dir}/debug_data_canada.csv',index=True)
    df = df.reset_index().set_index('date')
    _find_inflection(df)
    # inflection_result,inflection_date,slope_max = _find_inflection(df)
    # inflection_df = pd.DataFrame(inflections,columns=['country_region','province_state','inflection_result','inflection_date','slope_max'])
    # inflection_df = inflection_df.set_index('country_region')

    # inflection_df.to_sql('inflection',if_exists='replace',index=True,con=engine)
    # inflection_df.to_csv(f'{processed_dir}/{csv_filename}',index=True)

if __name__ == "__main__":
    store_all()