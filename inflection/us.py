from sqlalchemy import create_engine
import pandas as pd
from datetime import timedelta
import numpy as np
from scipy.stats import linregress
from inflection._inflection_analyzer import(_find_inflection)

processed_dir = 'processed_data'
db_filename = 'data_us.db'
csv_filename = 'inflection_data_us.csv'
engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')


def data_grabber(province_state,county):
    if county: 
        df = pd.read_sql(f"SELECT date,mortality FROM raw_data where province_state == "+ '"'+ province_state +'"' + ' and ' + 'county ==' + '"'+ county +'"',con=engine,index_col= 'date', parse_dates=['date'])
    else:
        df = pd.read_sql(f"SELECT date,mortality FROM raw_data where province_state == "+ '"'+ province_state +'"' + ' and ' + 'county IS NULL',con=engine,index_col= 'date', parse_dates=['date'])
    
    return df

def store_all():

    province_states = pd.read_sql(f"SELECT province_state FROM raw_data",con=engine)
    province_states = province_states.province_state.unique()
    
    inflections = []
    for province_state in province_states:
        counties = pd.read_sql(f"SELECT county FROM raw_data where province_state  == "+ '"'+ province_state +'"',con=engine)
        counties = counties.county.unique()
        for county in counties:
            if county != 'Unassigned':
                print(province_state,county)
                df = data_grabber(province_state,county)
                inflection_result,inflection_date,slope_max,trust_index = _find_inflection(df)
                inflections.append([province_state,county,inflection_result,inflection_date,slope_max,trust_index])
           
    inflection_df = pd.DataFrame(inflections,columns=['province_state','county','inflection_result','inflection_date','slope_max','trust_index'])
    inflection_df = inflection_df.set_index('province_state')

    inflection_df.to_sql('inflection',if_exists='replace',index=True,con=engine)
    inflection_df.to_csv(f'{processed_dir}/{csv_filename}',index=True)

    print('Done ')
    
def testing():
    df = data_grabber('New Hampshire','Hillsborough')
    inflection_result,inflection_date,slope_max,trust_index = _find_inflection(df)
    print(inflection_result)

if __name__ == "__main__":
    store_all()