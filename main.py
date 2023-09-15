import pandas as pd
import funcition.queries as  q
from helpers.connection import connectionConfig
from funcition.getDbs import getDbs
from funcition.getLastDbUpdate import getLastInfoUpdate
from funcition.getInforme import getInforme
from time import time
import os
from flask_cors import CORS
import waitress
from helpers.Notifier import Notifier

from flask import Flask, send_file
from sqlalchemy import text

app = Flask(__name__)

CORS(app)

@app.route("/")
def home():
    try:
        res = {}
        # Devolvemos los estados de la aplicacion
        res = getLastInfoUpdate()
        if res == None:
            res = {}
            res["status"] = "error"
            res["error"] = "No se pudo obtener el estado de la aplicacion"
            return res
        return res 
    except Exception as e:
        Notifier("Error al obtener el estado de la aplicacion")
        res = {}
        res["status"] = "error"
        res["error"] = str(e)
        return res
    
@app.route("/getNewInfo")
def getNewInfo():
    try:
        res = {}
        # 1. tomamos los datos de la base de datos de flxx
        resDb = getDbs()
        # 2. Los guardamos en la base de datos local
        if resDb == None:
            res["error"] = "No se pudo guardar la informacion en la base de datos local"
            return res
        else:
            resInfo = getInforme()
            if resInfo == False:
                res["status"] = "error"
                res["error"] = "No se pudo crear el informe"
                return res
            else:
                res["status"] = "success"
                res["message"] = "Se creo el informe correctamente"
                return res
    except Exception as e:
        Notifier("Error al obtener el nuevo informe")
    # Hay que renderizar la pantalla de errores y mandarle el error correspondiente
        res = {}
        res["status"] = "error"
        res["error"] = str(e)
        return res

@app.route("/getLastInfo") 
def getLastInfo():
    try:
        filename = "ventas/Distrisuper informe ventas.xlsx"
        # chequear si el archivo existe
        # obtener el path completo del archivo
        path = os.path.abspath(filename)
        if os.path.isfile(filename):
            return path
            # return send_file(filename, as_attachment=True)
        else:
            return {"status": "error", "error": "No hay informes disponibles. Debe generar uno nuevo."}
    except Exception as e:
        Notifier("Error al descargar el último informe")
        return str(e)

waitress.serve(app, port=8045, host='127.0.0.1')