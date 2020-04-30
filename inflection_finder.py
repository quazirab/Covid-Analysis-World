from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import linregress


processed_dir = 'processed_data'
db_filename = 'data_country.db'
csv_filename = 'inflection_data_country.csv'
engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')


def based_on_country(country):
    df = pd.read_sql("SELECT * FROM raw_data WHERE country_region =="+ '"'+ country +'"',con=engine,parse_dates=['date'])
    df = df.groupby(['date']).sum()
    inflection_result,inflection_mortality = _find_inflection(df[df.mortality > 0]['mortality'].values)
    if inflection_result:
        inflection_date = df[df.mortality == inflection_mortality].index.date.tolist()[0]
        return inflection_result,inflection_date
    else:
        return 0, None

def _find_inflection(mortality_list):
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
        
        slopes = np.array(slopes)
        slope_ratio = slopes[-1]/slopes.max()
        if slope_ratio >= 1 :
            # return inflection and number of
            return 0,None
        else :
            return 1,mortality_list[np.argmax(slopes)]
    else:
        return 0,None

def store_all_country():
    countries = pd.read_sql(f"SELECT country_region FROM raw_data",con=engine)
    countries = countries.country_region.unique()
    
    results = []
    for country in countries:
        inflection_result,inflection_date = based_on_country(country)
        results.append([country,inflection_result,inflection_date])
    
    inflection_df = pd.DataFrame(results,columns=['country_region','inflection_result','inflection_date'])
    inflection_df = inflection_df.set_index('country_region')
    inflection_df.to_sql('inflection',if_exists='replace',index=True,con=engine)
    inflection_df.to_csv(f'{processed_dir}/{csv_filename}',index=True)

if __name__ == "__main__":
    # inflection_result,inflection_date = based_on_country('US')
    # print(inflection_result,inflection_date)   
    store_all_country()