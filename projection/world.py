from projection.hyperbolic_projection import hyperbolic_projection
from sqlalchemy import create_engine
import pandas as pd

processed_dir = 'processed_data'
db_filename = 'data_country.db'

engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')

def data_grabber(country_region,province_state,inflection_date,slope_max):
    if province_state: 
        df = pd.read_sql(f"SELECT date,mortality FROM raw_data where country_region == "+ '"'+ country_region +'"' + ' and ' + 'province_state ==' + '"'+ province_state +'"',con=engine,parse_dates=['date'])
    else:
        df = pd.read_sql(f"SELECT date,mortality FROM raw_data where country_region == "+ '"'+ country_region +'"' + ' and ' + 'province_state IS NULL',con=engine,parse_dates=['date'])
    df = df[df.mortality > 0]
    return df

def store_all():
    inflection_df = pd.read_sql(f"SELECT * FROM inflection where inflection_result==1 and trust_index==1.0",parse_dates=['inflection_date'],con=engine)
    
    # delete existing 
    
    for index,row in inflection_df.iterrows():
        print(row.country_region,row.province_state)
        df = data_grabber(row.country_region,row.province_state,row.inflection_date,row.slope_max)
        df = hyperbolic_projection(df,row.inflection_date,row.slope_max)


def _testing():
    inflection_df = pd.read_sql(f"SELECT * FROM inflection where inflection_result==1 and country_region=='Italy' and province_state IS NULL",parse_dates=['inflection_date'],con=engine)
    
    for index,row in inflection_df.iterrows():
        if row.country_region != 'United Kingdom':
            df = data_grabber(row.country_region,row.province_state,row.inflection_date,row.slope_max)
            print(row.country_region,row.province_state)
            hyperbolic_projection(df,row.inflection_date,row.slope_max)
    exit()

if __name__ == "__main__":
    store_all()