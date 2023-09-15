import dotenv
from sqlalchemy import create_engine

config = dotenv.dotenv_values()

def connectionLocal():
    try:
        return create_engine("sqlite:///helpers/dbs/dataFlxx.sqlite", echo=True)
    except Exception as e:  
        print(e)

def connectionDev():
    try:
        return create_engine("sqlite:///helpers/dbs//dataFlxx.sqlite", echo=True)
    except Exception as e:  
        print(e)

def connectionConfig():
    try:
        return create_engine("sqlite:///helpers/dbs//config.sqlite", echo=True)
    except Exception as e:  
        print(e)