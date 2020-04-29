from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import linregress


processed_dir = 'processed_data'
db_filename = 'raw_data_country.db'

engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')


def based_on_country(country):
    df = pd.read_sql(f"SELECT * FROM raw_data WHERE country_region == '{country}'",con=engine,parse_dates=['date'])
    df = df.groupby(['date']).sum()
    inflation = _find_inflation(df[df.mortality > 0]['mortality'].values)
    

def _find_inflation(mortality_list):
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

    if slopes[-1]/slopes.max() > 1 :
        # return inflation and number of 
        return 0
    else :
        return 1


if __name__ == "__main__":
    based_on_country('United Kingdom')