
from scipy.stats import linregress
from datetime import timedelta
import pandas as pd
import time

def _find_inflection(df):
    df = df[df.ne(0).idxmax().mortality:]
    # mortality max needs to be more than or equal to 10 and have more than 5 results
    if df.shape[0] < 5 or df.mortality.max() < 10:
        return -1,None,None,None

    df = df.reset_index()
    df['day_number'] = df.index + 1
    df = df.set_index('date')
    
    indexes_to_compute = pd.date_range(start=df.index[4],end=df.index[-1])
    for index_to_compute in indexes_to_compute:
        try:
            df_for_slope = df.loc[index_to_compute-timedelta(days=4):index_to_compute]
            # print(df_for_slope)
        except KeyError:
            return -1, None,None,None
        
        try:
            slope, _, _, _, _ = linregress(df_for_slope.day_number,df_for_slope.mortality)    
            df.loc[index_to_compute,'slope'] = slope
        except Exception as e :
            print(e)
            exit()
    
    slope_max_date = df.slope.idxmax().date()
    trust_index = df.index[-1].date() - slope_max_date
    trust_index = trust_index.days/10
    if trust_index > 1:
        trust_index = 1.0

    if df.slope.idxmax().date() == df.index[-1]:
        return 0,None,None,None
    else:
        return 1,slope_max_date,df.slope.max(),trust_index