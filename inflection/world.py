from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from inflection._inflection_analyzer import(_find_inflection)

processed_dir = 'processed_data'
db_filename = 'data_country.db'
csv_filename = 'inflection_data_world.csv'
engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')


def data_grabber(country_region,province_state):
    if province_state: 
        df = pd.read_sql(f"SELECT date,mortality FROM raw_data where country_region == "+ '"'+ country_region +'"' + ' and ' + 'province_state ==' + '"'+ province_state +'"',con=engine,index_col= 'date', parse_dates=['date'])
    else:
        df = pd.read_sql(f"SELECT date,mortality FROM raw_data where country_region == "+ '"'+ country_region +'"' + ' and ' + 'province_state IS NULL',con=engine,index_col= 'date', parse_dates=['date'])
    
    return df

def store_all():
    country_regions = pd.read_sql(f"SELECT country_region FROM raw_data",con=engine)
    country_regions = country_regions.country_region.unique()
    
    inflections = []
    for country_region in country_regions:
        province_states = pd.read_sql(f"SELECT province_state FROM raw_data where country_region == "+ '"'+ country_region +'"',con=engine)
        province_states = province_states.province_state.unique()

        for province_state in province_states:
            df = data_grabber(country_region,province_state)
            inflection_result,inflection_date,slope_max,trust_index = _find_inflection(df)
            inflections.append([country_region,province_state,inflection_result,inflection_date,slope_max,trust_index])

    inflection_df = pd.DataFrame(inflections,columns=['country_region','province_state','inflection_result','inflection_date','slope_max','trust_index'])
    inflection_df = inflection_df.set_index('country_region')

    inflection_df.to_sql('inflection',if_exists='replace',index=True,con=engine)
    inflection_df.to_csv(f'{processed_dir}/{csv_filename}',index=True)

if __name__ == "__main__":
    store_all()