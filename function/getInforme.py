from helpers.connection import connectionLocal, connectionConfig
import pandas as pd
import function.queries as q
from time import time
import os
from sqlalchemy import text

def getInforme(condition = ["PPAL", "DS"], dbs = ["DISTRI", "DIMES"]):
    """
        Esta funcion genera el informe de ventas de los ultimos 6 meses
    """

    connLocal = connectionLocal()
    # TOMAMOS LA SIGUIENTE INFORMACION:
    # 1. Bonificacion general por cliente
    # 2. Nombre de la marca
    # 3. Descuento por marca del cliente
    # 4. Total de ventas por cliente

    # ARR DBS
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
            #     
            #    

    periods = [["ultimos_180_dias", "2023-03-07 00:00:00", "2023-09-06 23:59:59"]]


    
    try:
        for period in periods:
            print(f"Periodo: {period[0]}")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
            # for dbs in ARR_DBS:
            begin = time()
            new_data = pd.DataFrame(columns=["CODIGOPARTICULAR", "RAZONSOCIAL",  "BONIFICACION", "CODIGOMARCA", "CODIGOCLIENTE", "DESCRIPCION", f'VENTAS_{period[0].upper()}', f'COSTO_{period[0].upper()}', "PORCENTAJEDESCUENTO", "MARGEN"])
            for db in dbs:
                for type in condition:
                    db_params = ARR_DBS[db][type]
                    print(f"Base de datos: {db_params[0]}")
                    print(q.getDataPrevia(db_params[1], db_params[2], period=period[0].upper(), from_date=period[1], to_date=period[2]))
                    data_previa = pd.read_sql(q.getDataPrevia(db_params[1], db_params[2], period=period[0].upper(), from_date=period[1], to_date=period[2]), con=connLocal)
                    data_previa['CODIGOMARCA'] = data_previa['CODIGOMARCA'].astype(str)
                    data_previa['RAZONSOCIAL'] = data_previa['RAZONSOCIAL'].astype(str)
                    data_previa = data_previa.dropna()
                    markups = pd.read_sql(q.getMarkups, con=connLocal)
                    markups['CODIGOMARCA'] = markups['CODIGOMARCA'].astype(str)
                    # print(markups["CODIGOMARCA"])
                    # data_previa = data_previa.merge(markups, how="left", on="CODIGOMARCA")
                    # Hacemos un cruce con los descuentos de cada cliente
                    raw_data_clients_discounts = pd.read_sql(q.getAllDescuentosLocal, con=connLocal)
                    # print(data_previa.dtypes)
                    # print(descuentos.dtypes)

                    # print(new_data.dtypes)
                    # for to interate over discounts

                    # brands Distri
                    brands_distri = pd.read_sql(q.getAllBrandsDistri, con=connLocal)

                    # total clients 
                    total_clients = data_previa['CODIGOCLIENTE'].unique()

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
                                sales = 0
                                cost = 0
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

                                # We get the sales and cost of the client and brand
                                if not prev_data_client_brand.empty:
                                    sales = prev_data_client_brand[f'VENTAS_{period[0].upper()}'].values[0] if prev_data_client_brand[f'VENTAS_{period[0].upper()}'].values[0] != None else 0
                                    cost = prev_data_client_brand[f'COSTO_{period[0].upper()}'].values[0] if prev_data_client_brand[f'COSTO_{period[0].upper()}'].values[0] != None else 0

                                # we get the martkup of the brand
                                markup = markups[(markups['CODIGOMARCA'] == brand)]
                                if not markup.empty:
                                    margin = markup['MARGEN'].values[0]


                                if not prev_data_client_brand.empty:
                                    new_row = {
                                        "CODIGOPARTICULAR": "{:05}".format(particular_code),
                                        "RAZONSOCIAL": company_name,
                                        "BONIFICACION": bonfication,
                                        "CODIGOMARCA": "{:03}".format(brand) if brand != '80' else brand,
                                        "CODIGOCLIENTE": "{:05}".format(client_code),
                                        "DESCRIPCION": brand_desc,
                                        f'VENTAS_{period[0].upper()}':  sales, 
                                        f'COSTO_{period[0].upper()}': cost, 
                                        "PORCENTAJEDESCUENTO": discount, 
                                        "MARGEN": margin
                                    }
                                    new_data.loc[len(new_data)] = new_row
                                else: 
                                    new_row = {
                                        "CODIGOPARTICULAR": "{:05}".format(particular_code),
                                        "RAZONSOCIAL": company_name,
                                        "BONIFICACION": bonfication,
                                        "CODIGOMARCA": "{:03}".format(brand) if brand != None else None,
                                        "CODIGOCLIENTE": "{:05}".format(client_code),
                                        "DESCRIPCION": brand_desc,
                                        f'VENTAS_{period[0].upper()}':  0, 
                                        f'COSTO_{period[0].upper()}': 0, 
                                        "PORCENTAJEDESCUENTO": discount, 
                                        "MARGEN": margin
                                    }
                                    new_data.loc[len(new_data)] = new_row
            
            # Change type of columns
            new_data['BONIFICACION'] = new_data['BONIFICACION'].str.replace(',', '.').astype(float)

            new_data = new_data[['RAZONSOCIAL','BONIFICACION', 'DESCRIPCION', 'PORCENTAJEDESCUENTO', f'VENTAS_{period[0].upper()}', f'COSTO_{period[0].upper()}', "MARGEN"]]
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