from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from scipy.stats import linregress


processed_dir = 'processed_data'
db_filename = 'data_country.db'

engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')

def hyperbolic_projection():
    df = pd.read_sql(f"SELECT * FROM inflection where inflection_result == 1",con=engine)
    for 




if __name__ == "__main__":
    hyperbolic_projection()