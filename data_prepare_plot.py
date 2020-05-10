import pandas as pd
from sqlalchemy import create_engine


processed_dir = 'processed_data'

def global_prepare():
    db_filename = 'data_country.db'
    csv_filename = 'plot_data_global.csv'
    engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')
    df_global_raw_data = pd.read_sql(f"SELECT * FROM raw_data",con=engine,parse_dates=['date'])
    df_global_inflection = pd.read_sql(f"SELECT * FROM inflection",parse_dates=['inflection_date'],con=engine).drop(columns=['slope_max'])
     

    df = pd.merge(df_global_raw_data,df_global_inflection,how='left',left_on=['country_region','province_state'],right_on=['country_region','province_state']).set_index('date')
    
    df.to_csv(f'{processed_dir}/{csv_filename}',index=True)


def us_prepare():
    db_filename = 'data_us.db'
    csv_filename = 'plot_data_us.csv'
    engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')
    
    df_us_raw_data = pd.read_sql(f"SELECT * FROM raw_data",con=engine,index_col='date',parse_dates=['date'])
    df_us_projection_data = pd.read_sql(f"SELECT * FROM projection_data",con=engine,index_col='date',parse_dates=['date'])

    df_data = df_us_raw_data.append(df_us_projection_data)
    
    df_us_inflection = pd.read_sql(f"SELECT * FROM inflection",parse_dates=['inflection_date'],con=engine).drop(columns=['slope_max'])
    
    df_data = df_data.reset_index()
    df = pd.merge(df_data,df_us_inflection,how='left',left_on=['county','province_state'],right_on=['county','province_state']).set_index('date')
    
    # exit()
    df.to_csv(f'{processed_dir}/{csv_filename}',index=True)

if __name__ == "__main__":
    # global_prepare()
    us_prepare()