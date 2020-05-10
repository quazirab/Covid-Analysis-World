import os

def reset(engine,relative_dir_csv):
    with engine.connect() as connection:
        result = connection.execute("DROP TABLE IF EXISTS projection_data")
    
    if os.path.exists(relative_dir_csv):
        os.remove(relative_dir_csv)
