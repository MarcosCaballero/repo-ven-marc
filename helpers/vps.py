import dotenv
from sqlalchemy import create_engine
import mysql.connector

config = dotenv.dotenv_values()

def connectionVPS69():
    try:
        return mysql.connector.connect(
            host=config["LUPITA_HOST"],
            port=3306,
            user=config["LUPITA_USER"],
            database=config["LUPITA_DATABASE"],
            password=config["LUPITA_PASSWORD"])
    except Exception as e:  
        print(e)
