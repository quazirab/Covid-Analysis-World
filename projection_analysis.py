from sqlalchemy import create_engine
import pandas as pd
import matplotlib.pyplot as plt

processed_dir = 'processed_data'
db_filename = 'raw_data_country.db'

engine = create_engine(f'sqlite:///{processed_dir}/{db_filename}')


def based_on_country(country):
    df = pd.read_sql(f"SELECT * FROM raw_data WHERE country_region == '{country}'",con=engine,parse_dates=['date'])
    df = df.groupby(['date']).sum()

    print(type(df.index.values[0]))
    print(df)
    plt.plot(df.index.values , df.cases.values)
    plt.show()

if __name__ == "__main__":
    based_on_country('US')