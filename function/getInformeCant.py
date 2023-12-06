from helpers.connection import connectionLocal, connectionConfig
import function.queries as q

import pandas as pd
from time import time
import os
from sqlalchemy import text
from datetime import datetime
from dateutil.relativedelta import relativedelta
import calendar
from helpers.Notifier import Notifier

ARR_DBS = {
                "DISTRI": {
                    "DS": ["ventas_distri_ds","cabezacomprobantes_distri_ds", "cuerpocomprobantes_distri_ds"],
                    "PPAL": ["ventas_distri_ppal", "cabezacomprobantes_distri_ppal", "cuerpocomprobantes_distri_ppal"],
                },
                "DIMES": {
                    "PPAL": ["ventas_dimes_ppal", "cabezacomprobantes_dimes_ppal", "cuerpocomprobantes_dimes_ppal"],
                    "DS": ["ventas_dimes_ds","cabezacomprobantes_dimes_ds", "cuerpocomprobantes_dimes_ds"]
                }
            }  

periods = [["ultimos_30_dias", "2023-10-01 00:00:00", "2023-11-31 23:59:59"]]

    
meses_espanol = [
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre"
]



# Deprecado pero se guarda por las dudas
def getInformeCant(condition = ["PPAL", "DS"], dbs = ["DISTRI", "DIMES"]):
    """
        Esta funcion genera el informe de ventas de los ultimos 6 meses
    """

    connLocal = connectionLocal()
    
    def getDates():
        initMonth = periods[0][1][:7]
        print(initMonth)
        endMonth = periods[0][2][:7]
        initMonth = f"{initMonth[5:7]}-{initMonth[0:4]}"
        endMonth =  f"{endMonth[5:7]}-{endMonth[0:4]}"
        
        months = []

        initMonth = datetime.strptime(initMonth, "%m-%Y")
        endMonth = datetime.strptime(endMonth, "%m-%Y")
        while initMonth <= endMonth:
            months.append(initMonth.strftime("%m-%Y"))
            initMonth = initMonth + relativedelta(months=1)
        return months
        
    def getMonthsQuery(months):
        query = ""
        for i in range(len(months)):
            month = months[i][0:2]
            year = months[i][3:7]
            query += f""" SUM(CASE 
           WHEN strftime('%m', cdp2.FECHACOMPROBANTE) = '{month}' AND strftime('%Y', cdp2.FECHACOMPROBANTE) = '{year}' THEN cdp.cantidad
           ELSE 0 END) AS '{meses_espanol[int(month)-1].upper()}',"""
        return query[:-1]


    try:
        dates = getMonthsQuery(getDates())
        for period in periods:
            print(f"Periodo: {period[0]}")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
            # for dbs in ARR_DBS:
            begin = time()
            new_data = pd.DataFrame(columns=["CODIGOPARTICULAR", "RAZONSOCIAL",  "BONIFICACION", "CODIGOMARCA", "CODIGOCLIENTE", "DESCRIPCION", "PORCENTAJEDESCUENTO", "MARGEN"])

            # Añadimos las columnas de los meses
            months = getDates()
            months = [f"{meses_espanol[int(month[0:2])-1].upper()}" for month in months]
            for i in range(len(months)):
                new_data[f"{months[i]}"] = 0 #Agregar los años


            # definimos las columnas del informe final
            columns = ["CODIGOPARTICULAR", "RAZONSOCIAL", "BONIFICACION", "CODIGOMARCA", "CODIGOCLIENTE", "DESCRIPCION", "PORCENTAJEDESCUENTO"]
            columns.extend(months)
            columns.extend(["MARGEN"])

            # Trabajamos con cada una de las DBs
            for db in dbs:
                for type in condition:
                    db_params = ARR_DBS[db][type]
                    print(f"Base de datos: {db_params[0]}")
                    
                    # print(q.getDataPreviaCant(db_params[1], db_params[2], dates,  period=period[0].upper(), from_date=period[1], to_date=period[2]))
                    # Tomamos la data de la DBs
                    data_previa = pd.read_sql(q.getDataPreviaCant(db_params[1], db_params[2], dates, period=period[0].upper(), from_date=period[1], to_date=period[2]), con=connLocal)
                    markups = pd.read_sql(q.getMarkups, con=connLocal)
                    # Cambiamos el tipo de dato de las columnas
                    data_previa['CODIGOMARCA'] = data_previa['CODIGOMARCA'].astype(str)
                    data_previa['RAZONSOCIAL'] = data_previa['RAZONSOCIAL'].astype(str)
                    markups['CODIGOMARCA'] = markups['CODIGOMARCA'].astype(str)
                    # Borramos las columnas vacias
                    data_previa = data_previa.dropna()
                    
                    # Tomamos la data de los descuentos 
                    raw_data_clients_discounts = pd.read_sql(q.getAllDescuentosLocal, con=connLocal)

                    # Tomamos la data de las marcas
                    brands_distri = pd.read_sql(q.getAllBrandsDistri, con=connLocal)

                    # Tomamos la data unica de los clientes que se encuentran en la data previa
                    total_clients = data_previa['CODIGOCLIENTE'].unique()

                    # Recorremos la lista de clientes y la recorremos para sacar la data de cada cliente
                    for i in range(len(total_clients)):
                        print(i)
                        client_code = total_clients[i]
                        prev_data_client = data_previa[(data_previa['CODIGOCLIENTE'] == client_code)]
                        #print(prev_data_client)
                        # At least the client has to have one row in data_previa
                        if not prev_data_client.empty:
                            company_name = prev_data_client['RAZONSOCIAL'].values[0]
                            bonfication = prev_data_client['BONIFICACION'].values[0]
                            particular_code = prev_data_client['CODIGOPARTICULAR'].values[0]
                            # We get all the discounts of the client
                            discounts = raw_data_clients_discounts[(raw_data_clients_discounts['CODIGOCLIENTE'] == client_code)]
                            # We pass through all the brands also if the client has not bought that brand
                            for j in range(len(brands_distri)):
                                # INITIALIZE VARIABLES
                                discount = 0
                                margin = 0

                                brand = brands_distri.loc[j, "CODIGOMARCA"]
                                brand_desc = brands_distri.loc[j, "DESCRIPCION"]
                                # We get the data of the client and the brand of prev_data_client if exists we get the data 
                                # and we add it to new_data, if not we add a row with 0 in sales and cost
                                prev_data_client_brand = prev_data_client[(prev_data_client['CODIGOMARCA'] == brand)]

                                if not discounts.empty:
                                    discount_brand = discounts[(discounts['CODIGOMARCA'] == brand)]
                                    if not discount_brand.empty:
                                        discount = discount_brand['PORCENTAJEDESCUENTO'].values[0]
                                    else: 
                                        discount = 0
                                
                                # we get the martkup of the brand
                                markup = markups[(markups['CODIGOMARCA'] == brand)]
                                if not markup.empty:
                                    margin = markup['MARGEN'].values[0]

                                # id de fila nueva 
                                new_row_id = len(new_data)


                                new_row = {
                                    "CODIGOPARTICULAR": "{:05}".format(particular_code),
                                    "RAZONSOCIAL": company_name,
                                    "BONIFICACION": bonfication,
                                    "CODIGOMARCA": "{:03}".format(brand) if brand != '80' else brand,
                                    "CODIGOCLIENTE": "{:05}".format(client_code),
                                    "DESCRIPCION": brand_desc,
                                    "PORCENTAJEDESCUENTO": discount, 
                                    "MARGEN": margin
                                }

                                new_data.loc[new_row_id] = new_row

                                for m in range(len(months)):
                                    # We get the sales and cost of the client and brand
                                    if not prev_data_client_brand.empty:
                                        # sales = prev_data_client_brand[f'VENTAS_{period[0].upper()}'].values[0] if prev_data_client_brand[f'VENTAS_{period[0].upper()}'].values[0] != None else 0
                                        # cost = prev_data_client_brand[f'COSTO_{period[0].upper()}'].values[0] if prev_data_client_brand[f'COSTO_{period[0].upper()}'].values[0] != None else 0
                                        units = prev_data_client_brand[f'{months[m]}'].values[0] if prev_data_client_brand[f'{months[m]}'].values[0] != None else 0
                                        
                                        new_data.loc[new_row_id,f'{months[m]}'] = units
                                    else: 
                                        new_data.loc[new_row_id,f'{months[m]}'] = 0
            
            # Change type of columns
            new_data['BONIFICACION'] = new_data['BONIFICACION'].str.replace(',', '.').astype(float)

            new_data = new_data[columns]
            ruta = os.path.join(os.getcwd(), 'ventas'+"/"+period[0])
            if not os.path.exists(ruta):
                os.makedirs(ruta)
            # Save the info in excel
            new_data.to_excel(f"ventas/Distrisuper informe ventas.xlsx", engine="openpyxl",  index=False)
            end = time()
            print(f"Se ha guardado el archivo todo en {end-begin} segundos")
            # response
            connLocal = connectionConfig().connect()
            stmt = text("INSERT INTO info_updates (status) VALUES ('Terminado')")
            connLocal.execute(stmt)
            connLocal.commit()
            # get the last item
            res = pd.read_sql(q.getLastUpdateInfo, con=connLocal)
            res = res.to_dict(orient="records")[0]
            connLocal.close()
            
            return res

    except Exception as e:
        print(e)
        connLocal = connectionConfig().connect()
        stmt = text("INSERT INTO info_updates (status) VALUES ('Error')")
        connLocal.execute(stmt)
        connLocal.commit()
        connLocal.close()
        return False
    

def getInformeCantMonth(date, phone = ['5492235385084'], brands = [], pp = 0):
    """
        Esta funcion genera el informe de ventas de los ultimos 6 meses
    """

    condition = ["PPAL", "DS"]
    dbs = ["DISTRI", "DIMES"]

    connLocal = connectionLocal()
    
    print(date)

    def getWeeks():
        month = int(date[5:7])
        year = int(date[0:4])
            # Obtener el calendario del mes
        cal = calendar.monthcalendar(year, month)

        # Inicializar un conjunto para almacenar las semanas
        weeks = set()

        # Iterar sobre cada semana del mes
        for week in cal:
            for day in week:
                # Si el día no es 0 (fuera del mes)
                if day != 0:
                    # Obtener la semana ISO del día y agregarla al conjunto
                    week_iso = datetime(year, month, day).isocalendar()[1]
                    weeks.add(week_iso)

        return sorted(list(weeks))    

    print(getWeeks())
        
    def getWeeksQuery(weeks):
        query = ""
        for i in range(len(weeks)):
            week = weeks[i]
            year = date[0:4]
            query += f""" SUM(CASE 
           WHEN strftime('%W', cdp2.FECHACOMPROBANTE) = '{week}' AND strftime('%Y', cdp2.FECHACOMPROBANTE) = '{year}' THEN cdp.cantidad
           ELSE 0 END) AS 'SEMANA Nro {i + 1}',"""
        return query[:-1]
    
    def getBrandsQuery(brands):
        strBrands = ""
        for i in range(len(brands)):
            strBrands += f"'{brands[i]}',"

        return strBrands[:-1]

    brands = getBrandsQuery(brands)
    print(brands)
    # def getWeeksCampaign():
    #     resCamp = pd.read_sql(q.getCreatedCampanas(date), con=connLocal)
    #     idCamp = resCamp['id'].values[0]
    #     weeks = {}
        
    #     for i in range(len(resCamp)):
    #         weeks = resCamp['created_at'].values[i]
    #         cal = calendar.monthcalendar(int(date[0:4]), int(date[5:7]))
        
    try:
        query_weeks = getWeeksQuery(getWeeks())
        for period in periods:
            print(f"Periodo: {period[0]}")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
            # for dbs in ARR_DBS:
            begin = time()
            # Añadimos las columnas de los meses
            weeks = getWeeks()
            columns = ["CODIGOPARTICULAR", "RAZONSOCIAL", "BONIFICACION", "PP", "CODIGOMARCA", "CODIGOCLIENTE", "DESCRIPCION", "PORCENTAJEDESCUENTO"]
            columns.extend(f"SEMANA Nro {i + 1}" for i in range(len(weeks)))
            columns.extend(["TOTAL UNIDADES", "VENTAS", "COSTOS", "RENTABILIDAD","MARGEN"])
            new_data = pd.DataFrame(columns=columns)
            # Trabajamos con cada una de las DBs
            all_total_units = 0
            all_sales = 0
            all_cost = 0
            all_rent = 0
            coef_pp = 1 - pp
            for db in dbs:
                for type in condition:
                    db_params = ARR_DBS[db][type]
                    print(f"Base de datos: {db_params[0]}")
                    
                    # Tomamos la data de la DBs
                    # print(q.getDataPreviaCant(db_params[1], db_params[2], query_weeks, date, pp, period=period[0].upper()))
                    data_previa = pd.read_sql(q.getDataPreviaCant(db_params[1], db_params[2], query_weeks, date, pp, period=period[0].upper()), con=connLocal)
                    markups = pd.read_sql(q.getMarkups, con=connLocal)
                    # Cambiamos el tipo de dato de las columnas
                    data_previa['CODIGOMARCA'] = data_previa['CODIGOMARCA'].astype(str)
                    data_previa['RAZONSOCIAL'] = data_previa['RAZONSOCIAL'].astype(str)
                    markups['CODIGOMARCA'] = markups['CODIGOMARCA'].astype(str)
                    
                    # Borramos las columnas vacias
                    data_previa = data_previa.dropna()
                    
                    # Tomamos la data de los descuentos 
                    raw_data_clients_discounts = pd.read_sql(q.getAllDescuentosLocal, con=connLocal)

                    # Tomamos la data de las marcas
                    brands_distri = pd.read_sql(q.getAllBrandsDistri(brands), con=connLocal)

                    # Tomamos la data unica de los clientes que se encuentran en la data previa
                    total_clients = data_previa['CODIGOCLIENTE'].unique()

                    # Recorremos la lista de clientes y la recorremos para sacar la data de cada cliente
                    for i in range(len(total_clients)):
                        print(i)
                        client_code = total_clients[i]
                        prev_data_client = data_previa[(data_previa['CODIGOCLIENTE'] == client_code)]
                        # At least the client has to have one row in data_previa
                        if not prev_data_client.empty:
                            company_name = prev_data_client['RAZONSOCIAL'].values[0]
                            bonfication = prev_data_client['BONIFICACION'].values[0]
                            particular_code = prev_data_client['CODIGOPARTICULAR'].values[0]
                            # We get all the discounts of the client
                            discounts = raw_data_clients_discounts[(raw_data_clients_discounts['CODIGOCLIENTE'] == client_code)]
                            # We pass through all the brands also if the client has not bought that brand
                            for j in range(len(brands_distri)):
                                # INITIALIZE VARIABLES
                                discount = 0
                                margin = 0
                                total_units = 0
                                costo = 0
                                venta = 0
                                rentabilidad = 0

                                brand = brands_distri.loc[j, "CODIGOMARCA"]
                                brand_desc = brands_distri.loc[j, "DESCRIPCION"]
                                # We get the data of the client and the brand of prev_data_client if exists we get the data 
                                # and we add it to new_data, if not we add a row with 0 in sales and cost
                                prev_data_client_brand = prev_data_client[(prev_data_client['CODIGOMARCA'] == brand)]
                                total_units = prev_data_client_brand['TOTAL UNIDADES'].values[0] if not prev_data_client_brand.empty else 0
                                costo = prev_data_client_brand['COSTOS'].values[0] if not prev_data_client_brand.empty else 0
                                venta = prev_data_client_brand['VENTAS'].values[0] if not prev_data_client_brand.empty else 0
                                rentabilidad = prev_data_client_brand['RENTABILIDAD'].values[0] if not prev_data_client_brand.empty else 0

                                if not discounts.empty:
                                    discount_brand = discounts[(discounts['CODIGOMARCA'] == brand)]
                                    if not discount_brand.empty:
                                        discount = discount_brand['PORCENTAJEDESCUENTO'].values[0]
                                    else: 
                                        discount = 0
                                
                                # we get the martkup of the brand
                                markup = markups[(markups['CODIGOMARCA'] == brand)]
                                if not markup.empty:
                                    margin = markup['MARGEN'].values[0]

                                if new_data.empty or new_data.loc[(new_data["CODIGOPARTICULAR"] == particular_code) & (new_data["CODIGOMARCA"] == brand)].empty:
                                    # id de fila nueva 
                                    new_row_id = len(new_data)
                                    
                                    all_total_units += total_units

                                    new_row = {
                                        "CODIGOPARTICULAR": "{:05}".format(particular_code),
                                        "RAZONSOCIAL": company_name,
                                        "BONIFICACION": bonfication,
                                        "PP": pp * 100,
                                        "CODIGOMARCA": "{:03}".format(brand) if brand != '80' else brand,
                                        "CODIGOCLIENTE": "{:05}".format(client_code),
                                        "DESCRIPCION": brand_desc,
                                        "PORCENTAJEDESCUENTO": discount, 
                                        "TOTAL UNIDADES": total_units,
                                        "VENTAS": venta,
                                        "COSTOS": costo,
                                        "RENTABILIDAD": rentabilidad,
                                        "MARGEN": margin
                                    }

                                    new_data.loc[new_row_id] = new_row

                                    for w in range(len(weeks)):
                                        # We get the sales and cost of the client and brand
                                        if not prev_data_client_brand.empty:
                                            # sales = prev_data_client_brand[f'VENTAS_{period[0].upper()}'].values[0] if prev_data_client_brand[f'VENTAS_{period[0].upper()}'].values[0] != None else 0
                                            # cost = prev_data_client_brand[f'COSTO_{period[0].upper()}'].values[0] if prev_data_client_brand[f'COSTO_{period[0].upper()}'].values[0] != None else 0
                                            units = prev_data_client_brand[f'SEMANA Nro {w + 1}'].values[0] if prev_data_client_brand[f'SEMANA Nro {w + 1}'].values[0] != None else 0

                                            new_data.loc[new_row_id,f'SEMANA Nro {w + 1}'] = units
                                        else: 
                                            new_data.loc[new_row_id,f'SEMANA Nro {w + 1}'] = 0
                                else: 
                                    new_row_id = new_data[(new_data["CODIGOPARTICULAR"]== particular_code) & (new_data["CODIGOMARCA"] == brand)].index[0]                                

                                    all_total_units += total_units

                                    new_data.loc[new_row_id, "TOTAL UNIDADES"] += total_units
                                    new_data.loc[new_row_id, "VENTAS"] += venta 
                                    new_data.loc[new_row_id, "COSTOS"] += costo
                                    # Recalculamos la nueva rentabilidad
                                    if(venta > 0 or costo > 0): # Si la venta o el costo es 0 entonces la rentabilidad sigue igual
                                        new_data.loc[new_row_id, "RENTABILIDAD"] = (new_data.loc[new_row_id, "VENTAS"] -  new_data.loc[new_row_id, "COSTOS"]) /  new_data.loc[new_row_id, "VENTAS"] * 100 

                                    for w in range(len(weeks)):
                                        # We get the sales and cost of the client and brand
                                        if not prev_data_client_brand.empty:
                                            # sales = prev_data_client_brand[f'VENTAS_{period[0].upper()}'].values[0] if prev_data_client_brand[f'VENTAS_{period[0].upper()}'].values[0] != None else 0
                                            # cost = prev_data_client_brand[f'COSTO_{period[0].upper()}'].values[0] if prev_data_client_brand[f'COSTO_{period[0].upper()}'].values[0] != None else 0
                                            units = prev_data_client_brand[f'SEMANA Nro {w + 1}'].values[0] if prev_data_client_brand[f'SEMANA Nro {w + 1}'].values[0] != None else 0
                                            prev_units = new_data[(new_data["CODIGOPARTICULAR"]== particular_code) & (new_data["CODIGOMARCA"] == brand)][f'SEMANA Nro {w + 1}'].values[0] if new_data[(new_data["CODIGOPARTICULAR"] == particular_code) & (new_data["CODIGOMARCA"] == brand)][f'SEMANA Nro {w + 1}'].values[0] != None else 0
                                            
                                            new_data.loc[new_row_id,f'SEMANA Nro {w + 1}'] = units + prev_units
                                        else: 
                                            new_data.loc[new_row_id,f'SEMANA Nro {w + 1}'] = 0
            
            # Change type of columns
            new_data['BONIFICACION'] = new_data['BONIFICACION'].str.replace(',', '.').astype(float)

            columns = ["CODIGOPARTICULAR", "RAZONSOCIAL", "BONIFICACION", "CODIGOMARCA", "CODIGOCLIENTE", "DESCRIPCION", "PORCENTAJEDESCUENTO", "PP"]
            columns.extend(f"SEMANA Nro {i + 1}" for i in range(len(weeks)))
            columns.extend(["TOTAL UNIDADES", "VENTAS", "COSTOS", "RENTABILIDAD","MARGEN"])

            new_data = new_data[columns]
            ruta = os.path.join(os.getcwd(), 'ventas'+"/"+period[0])
            if not os.path.exists(ruta):
                os.makedirs(ruta)
            # Save the info in excel
            filename = f"Distrisuper informe ventas {date} - {datetime.now().strftime('%d-%m-%Y %H-%M-%S')}.xlsx"
            
            new_data.to_excel(f"ventas/{filename}", engine="openpyxl",  index=False)
            print(f"Total unidades {all_total_units}")
            print(f"Total ventas {all_sales}")
            print(f"Total costos {all_cost}")
            print(f"Total rentabilidad {all_rent}")
            Notifier(f"Su informe fue generado con exito. Descarguelo con el nombre de Distrisuper informe ventas {date} - {datetime.now().strftime('%d-%m-%Y %H-%M-%S')}.xlsx", phone)           
            end = time()
            print(f"Se ha guardado el archivo todo en {end-begin} segundos")
            # response
            connLocal = connectionConfig().connect()
            stmt = text("INSERT INTO info_updates (status) VALUES ('Terminado')")
            connLocal.execute(stmt)
            connLocal.commit()
            # get the last item
            connLocal.close()
            
            return {"ok": 1, "data": filename}

    except Exception as e:
        print(e)
        connLocal = connectionConfig().connect()
        stmt = text("INSERT INTO info_updates (status) VALUES ('Error')")
        connLocal.execute(stmt)
        connLocal.commit()
        connLocal.close()
        return False
    