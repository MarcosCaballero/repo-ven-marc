import os 
from helpers.connection import connectionDev
import pandas as pd

def save_model_excel_return(filename, query):
    """
    Function to save a model in excel and return the path of the file 
    for download
    example:
    save_model_excel_return("test.xlsx", "SELECT * FROM clientes")
    return: path of the file for download
    """
    try:
        # We check if filename and query are not None
        if len(filename) == 0 or len(query) == 0:
            raise Exception("filename or query are None")
        connection = connectionDev()
        info_file = pd.read_sql(query, connection)
        info_file.to_csv(filename, index = False)
        path = os.path.join(os.getcwd(), "models")
        return path
    except Exception as e: 
        print(e)
        return None