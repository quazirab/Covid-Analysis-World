from projection.hyperbolic_projection import hyperbolic_projection
from sqlalchemy import create_engine
import pandas as pd
from projection.reset import reset

processed_dir = 'processed_data'
db_filename = 'data_us.db'
csv_filename = 'projection_data_us.csv'

engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')

def data_grabber(province_state,county,inflection_date,slope_max):
    if county: 
        df = pd.read_sql(f"SELECT date,mortality FROM raw_data where province_state == "+ '"'+ province_state +'"' + ' and ' + 'county ==' + '"'+ county +'"',con=engine,parse_dates=['date'])
    else:
        df = pd.read_sql(f"SELECT date,mortality FROM raw_data where province_state == "+ '"'+ province_state +'"' + ' and ' + 'county IS NULL',con=engine,parse_dates=['date'])
    df = df[df.mortality > 0]
    return df

def store_all():
    reset(engine,f'{processed_dir}/{csv_filename}')

    inflection_df = pd.read_sql(f"SELECT * FROM inflection where inflection_result==1 and trust_index==1.0",parse_dates=['inflection_date'],con=engine)

    for index,row in inflection_df.iterrows():
        print(row.province_state,row.county)
        df = data_grabber(row.province_state,row.county,row.inflection_date,row.slope_max)
        df = hyperbolic_projection(df,row.inflection_date,row.slope_max)
        df['province_state'] = row.province_state
        df['county'] = row.county
        df.to_sql('projection_data',if_exists='append',index=True,con=engine)

    df = pd.read_sql(f"SELECT * FROM projection_data",index_col= 'date', parse_dates=['date'],con=engine)
    df.to_csv(f'{processed_dir}/{csv_filename}',index=True)


if __name__ == "__main__":
    store_all()