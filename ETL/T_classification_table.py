# Load libraries
import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)

today = datetime.today()#.strftime("%d-%m-%Y")
now = datetime.now()#.strftime("%d-%m-%Y, %H:%M:%S")
path_to_file = r'H:\Mi unidad\Innk'
load_dotenv()



def categorize(df:pd.DataFrame)->pd.DataFrame:
    """Enumerates the fields in the classification table that are repeated for the same company
    
    Args:
        df (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """
    for cat in ['Problem', 'Solution']:
        counter_dict = {}
        for i in df.index:
            if df.loc[i, 'category'] == cat:
                company_id = df.loc[i, 'company_id']
                counter_dict[company_id] = counter_dict.get(company_id, 0) + 1
                df.loc[i, 'category'] = f'{cat} {counter_dict[company_id]}'
    return df


def main(path):
    """Main function that prepares the classification table for the database

    Args:
        path (text): path to raw file

    Returns:
        None: None
    """
    class_table = pd.read_json(path)
    class_table = class_table[['company_id', 'field', 'description', 'category']]
    class_table.rename(columns={'field': 'name'}, inplace=True)
    df = categorize(class_table)
    df.to_parquet(path_to_file + r'\T_classification.parquet')
    
    return None

if __name__=='__main__':
    file_path = sys.argv[1]
    main(file_path)

