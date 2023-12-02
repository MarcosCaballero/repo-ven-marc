import pandas as pd
import function.queries as  q
from helpers.connection import connectionConfig
from function.getDbs import getDbs
from function.getLastDbUpdate import getLastInfoUpdate
from function.getInforme import getInforme
from function.getInformeCant import getInformeCant, getInformeCantMonth
from time import time
import os
from flask_cors import CORS
import waitress
from helpers.Notifier import Notifier

from flask import Flask, send_file, request
from sqlalchemy import text
import re 

app = Flask(__name__)

CORS(app)

@app.route("/")
def home():
    try:
        res = {}
        # Devolvemos los estados de la aplicacion
        res = getLastInfoUpdate()
        if res["ok"] == 0:
            res = {}
            res["ok"] = 0
            res["error"] = {"details": "No se pudo obtener el estado de la aplicacion"}
            return res
        return res 
    except Exception as e:
        Notifier("Error al obtener el estado de la aplicacion")
        res = {}
        res["ok"] = 0
        res["error"] = {"details": str(e)}
        return res
    
@app.route("/update-db")
def updateDb():
    try:
        res = {}
        # 1. Tomamos los datos de la base de datos de flxx
        resDb = getDbs()
        # 2. Los guardamos en la base de datos local
        if resDb == None:
            res["ok"] = 0
            res["error"] = {"details": "No se pudo guardar la informacion en la base de datos local" }
            return res
        else:
            res["ok"] = 1
            res["data"] = "Se guardo la informacion correctamente"
            return res
    except Exception as e:
        Notifier("Error al actualizar la base de datos")
        res = {}
        res["ok"] = 0
        res["error"] = {"details": str(e)}
        return res

@app.route("/get-new-info")
def getNewInfo():
    try:
        resInfo = getInforme()
        if resInfo == False:
            res["ok"] = 0
            res["error"] = {"details": "No se pudo crear el informe"}
            return res
        else:
            res["ok"] = 1
            res["data"] = "Se creo el informe correctamente"
            return res
    except Exception as e:
        Notifier("Error al obtener el nuevo informe")
    # Hay que renderizar la pantalla de errores y mandarle el error correspondiente
        res = {}
        res["ok"] = 0
        res["error"] = {"details": str(e)}
        return res

@app.route("/new-info-cant", methods=["GET"])
def getNewInfoCant():
    try:
        res = {}
        # Tomamos la información de las fechas que viene por query params
        month = request.args.get("date")
        brands = request.args.getlist("brands[]")
        patron = re.compile(r"^\d{4}-\d{2}$")
        if patron.match(month):
            resInfo = getInformeCantMonth(month, brands)
            return resInfo
    except Exception as e:
        # Notifier("Error al obtener el nuevo informe de cantidades")
        res = {}
        res["ok"] = 0
        res["error"] = {"details": str(e)}
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
            return {"ok": 0, "error": {"details": "No se encontro el archivo"}}
    except Exception as e:
        Notifier("Error al descargar el último informe")
        return {"ok": 0, "error": {"details": str(e)}}

waitress.serve(app, port=8045, host='127.0.0.1')