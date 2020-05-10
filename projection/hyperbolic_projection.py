
from datetime import timedelta
from scipy.stats import linregress
import numpy as np
import pandas as pd

def hyperbolic_projection(df,inflection_date,slope_max):
    
    df = df.reset_index().drop(columns = 'index')
   
    df['day_number'] = df.index + 1
    df = df.set_index('date')
   
    df = df[df.index > (inflection_date - timedelta(days=4))]
    df.loc[inflection_date,'slope'] = slope_max

    indexes_to_compute = pd.date_range(start=inflection_date + timedelta(days=1),periods=9)
    
    for index_to_compute in indexes_to_compute:
        df_for_slope = df.loc[index_to_compute-timedelta(days=4):index_to_compute]
        slope, intercept = np.polyfit(df_for_slope.day_number,df_for_slope.mortality,1)
        df.loc[index_to_compute,'slope'] = slope
        df_for_slope.loc[index_to_compute,'slope'] = slope
    
    # Find slope3 of Slope 5
    df_for_slope_of_slope = df.loc[indexes_to_compute[-3]:indexes_to_compute[-1]]
    slope3_of_slope5, _, _, _, _ = linregress(df_for_slope_of_slope.day_number,df_for_slope_of_slope.slope)
    boost = 1.2133
    duration = (slope3_of_slope5/slope_max)*boost
    scale1 = df.loc[inflection_date.date()+timedelta(days=9)].mortality - df.loc[inflection_date.date()+timedelta(days=4)].mortality
    scale2_1 = np.tanh(duration*(df.loc[inflection_date.date()+timedelta(days=9)].day_number - df.loc[inflection_date.date()].day_number))
    scale2_2 = np.tanh(duration*(df.loc[inflection_date.date()+timedelta(days=9)].day_number - df.loc[inflection_date.date()+timedelta(days=4)].day_number))
    scale2 = scale2_1 - scale2_2
    scale = scale1/scale2
    average_of_days = df.loc[inflection_date.date()+timedelta(days=5):inflection_date.date()+timedelta(days=9)].day_number.mean()
    average_of_mortality = df.loc[inflection_date.date()+timedelta(days=5):inflection_date.date()+timedelta(days=9)].mortality.mean()
    day_number_at_slope_max = df.loc[inflection_date.date()].day_number
    d1 = average_of_days - day_number_at_slope_max
    f1 = scale*np.tanh(duration*d1)
    offset = average_of_mortality-f1
    # print(f'slope3_of_slope5 :  {slope3_of_slope5}')
    # print(f'duration :          {duration}')
    # print(f'scale1:             {scale1}')
    # print(f'scale2:             {scale2}')
    # print(f'scale:              {scale}')
    # print(f'average_of_days:    {average_of_days}')
    # print(f'average_of_mortality:{average_of_mortality}')
    # print(f'offset              :{offset}')

    last_date = df.index[-1].date()
    projection_indexes =  pd.date_range(start=last_date + timedelta(days=1),periods=14)
    projection_day_number = np.arange(df.loc[last_date].day_number+1,df.loc[last_date].day_number+15)
    df_projection = pd.DataFrame(index=projection_indexes)
    df_projection['day_number'] = projection_day_number
    for index,row in df_projection.iterrows():
        df_projection.loc[index,'projected_mortality'] = offset+scale*np.tanh(duration*(row.day_number-day_number_at_slope_max))

    df_projection.index.name = 'date'
    df_projection = df_projection.drop(columns='day_number')
    return df_projection