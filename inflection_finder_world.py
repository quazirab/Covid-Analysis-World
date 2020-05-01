from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import linregress


processed_dir = 'processed_data'
db_filename = 'data_country.db'
csv_filename = 'inflection_data_world.csv'
engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')


def inflection_finder(country_region,province_state):
    df = pd.read_sql(f"SELECT date,mortality FROM raw_data where country_region == "+ '"'+ country_region +'"' + ' and ' + 'province_state ==' + '"'+ province_state +'"',con=engine,parse_dates=['date'])
    
    df = df[df.mortality > 0]
    df = df.set_index('date')
    inflection_result,inflection_date,slope_max = _find_inflection(df['mortality'].values,df.index.date)

    if inflection_result:
        return inflection_result,inflection_date,slope_max
    else:
        return 0, None, None

def _find_inflection(mortality_list,date_list):

    if len(mortality_list)>4:
        mortality_array = []
        for i in range(len(mortality_list)):
            if i+5 < len(mortality_list):
                mortality_array.append(mortality_list[i:i+5])
        x = np.arange(5)
        slopes = []
        for mortality in mortality_array:
            slope, intercept, r_value, p_value, std_err = linregress(x,mortality)
            slopes.append(slope)
        
        if len(slopes) > 1: 
            slopes = np.array(slopes)
            slope_ratio = slopes[-1]/slopes.max()
        else:
            return 0,None,None

        if slope_ratio >= 1 :
            # return inflection and number of
            return 0,None,slopes.max()
        else :
            return 1,date_list[np.argmax(slopes)+4],slopes.max()
    else:
        return -1,None,None

def store_all():
    country_regions = pd.read_sql(f"SELECT country_region FROM raw_data",con=engine)
    country_regions = country_regions.country_region.unique()
    
    inflections = []
    for country_region in country_regions:
        province_states = pd.read_sql(f"SELECT province_state FROM raw_data where country_region == "+ '"'+ country_region +'"',con=engine)
        province_states = province_states.province_state.unique()
        for province_state in province_states:
            if province_state == None:
                province_state = 'Null'
            inflection_result,inflection_date,slope_max = inflection_finder(country_region,province_state)
            inflections.append([country_region,province_state,inflection_result,inflection_date,slope_max])

    inflection_df = pd.DataFrame(inflections,columns=['country_region','province_state','inflection_result','inflection_date','slope_max'])
    inflection_df = inflection_df.set_index('country_region')

    inflection_df.to_sql('inflection',if_exists='replace',index=True,con=engine)
    inflection_df.to_csv(f'{processed_dir}/{csv_filename}',index=True)

if __name__ == "__main__":
    store_all()