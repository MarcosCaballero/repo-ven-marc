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

from flask import Flask, send_file, request, jsonify
from sqlalchemy import text
import re 
import base64

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
            res["data"] = resInfo
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
        ppv = 0.17 #float(request.args.get("ppv"))
        ppc = 0 #float(request.args.get("ppc"))
        phone = request.args.getlist("phone[]")
        brands = request.args.getlist("brands[]")
        patron = re.compile(r"^\d{4}-\d{2}$")
        if patron.match(month):
            resInfo = getInformeCantMonth(month, phone, brands, ppv, ppc)
            return resInfo
    except Exception as e:
        # Notifier("Error al obtener el nuevo informe de cantidades")
        print(e)
        res = {}
        res["ok"] = 0
        res["error"] = {"details": str(e)}
        return res


@app.route("/download-info-cant") 
def downloadInfo():
    try:
        file = request.args.get("filename")
        ruta_xlsx = f'ventas/{file}'

        filename = os.path.join("ventas", file)

        if os.path.isfile(filename):            
            # Lee el contenido del archivo XLSX
            with open(ruta_xlsx, 'rb') as file:
                contenido_xlsx = file.read()

            # Codifica el contenido a base64
            base64_xlsx = base64.b64encode(contenido_xlsx).decode('utf-8')

            # Devuelve el base64 como respuesta JSON
            return jsonify({'ok': 1, 'data': base64_xlsx})
        else: 
            return {"ok": 0, "error": {"details": "No se encontraron archivos"}}
        
    except Exception as e:
        print(e)
        Notifier("Error al descargar el último informe")
        return {"ok": 0, "error": {"details": str(e)}}


@app.route("/inform-list") 
def getInfoList():
    try:
        filename = "ventas/"
        archivos = os.listdir(filename)
        archivos_con_fecha = [(archivo, os.path.getmtime(os.path.join(filename, archivo))) for archivo in archivos]
        archivos_ordenados = []

        for archivo in archivos_con_fecha:
            if archivo[0].startswith("Informe"):
                archivos_ordenados.append(archivo)


        if len(archivos_ordenados) > 0:
            return {"ok": 1, "data": archivos_ordenados}
        else:
            return {"ok": 0, "error": {"details": "No se encontraron archivos"}}
        
    except Exception as e:
        print(e)
        Notifier("Error al descargar el último informe")
        return {"ok": 0, "error": {"details": str(e)}}

        

waitress.serve(app, port=8045, host='127.0.0.1')