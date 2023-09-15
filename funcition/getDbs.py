import pandas as pd
import funcition.queries as  q
from helpers.connection import connectionLocal
from helpers.flxx import connectionDistriPPAL, connectionDistriDS, connectionDimesPPAL, connectionDimesDS

def getDbs():
    try:

        res = {}
        # CONEXIONES
        connFlxxDistri = connectionDistriPPAL()
        connFlxxDistriDS = connectionDistriDS()
        connFlxxDimes = connectionDimesPPAL()
        connFlxxDimesDS = connectionDimesDS()
        connLocal = connectionLocal()
        # TOMAMOS LOS CLIENTES DE DISTRI
        resClientes = pd.read_sql(q.getAllClientesDistri, con=connFlxxDistri)
        resClientes.to_sql("clientes_distri", con=connLocal, if_exists='replace', index=False)
        # TOMAMOS LAS MARCAS DE DISTRI
        resMarcas = pd.read_sql(q.getAllMarcasDistri, con=connFlxxDistri)
        resMarcas.to_sql("marcas_distri", con=connLocal, if_exists='replace', index=False)
        # TOMAMOS LAS VENTAS DE DISTRI PPAL
            # CABEZA
        resCabezaComprobantesDistriPPAL = pd.read_sql(q.getAllCabezaComprobantesDistriPPAL, con=connFlxxDistri)
        resCabezaComprobantesDistriPPAL.to_sql("cabezacomprobantes_distri_ppal", con=connLocal, if_exists='replace', index=False)
            # CUERPO
        resCuerpoComprobantesDistriPPAL = pd.read_sql(q.getAllCuerpoComprobantesDistriPPAL, con=connFlxxDistri)
        resCuerpoComprobantesDistriPPAL.to_sql("cuerpocomprobantes_distri_ppal", con=connLocal, if_exists='replace', index=False)
        # TOMAMOS LAS VENTAS DE DISTRI DS
            # CABEZA
        resCabezaComprobantesDistriDs = pd.read_sql(q.getCabezaComprobantesDistriDS, con=connFlxxDistriDS)
        resCabezaComprobantesDistriDs.to_sql("cabezacomprobantes_distri_ds", con=connLocal, if_exists='replace', index=False)
            # CUERPO
        resCuerpoComprobantesDistriDS = pd.read_sql(q.getCuerpoComprobantesDistriDS, con=connFlxxDistriDS)
        resCuerpoComprobantesDistriDS.to_sql("cuerpocomprobantes_distri_ds", con=connLocal, if_exists='replace', index=False)
        # TOMAMOS LAS VENTAS DE DIMES PPAL
            # CABEZA
        resCabezaComprobantesDimesPPAL = pd.read_sql(q.getCabezaComprobantesDimesPPAL, con=connFlxxDimes)
        resCabezaComprobantesDimesPPAL.to_sql("cabezacomprobantes_dimes_ppal", con=connLocal, if_exists='replace', index=False)
            # CUERPO
        resCuerpoComprobantesDimesPPAL = pd.read_sql(q.getCuerpoComprobantesDimesPPAL, con=connFlxxDimes)
        resCuerpoComprobantesDimesPPAL.to_sql("cuerpocomprobantes_dimes_ppal", con=connLocal, if_exists='replace', index=False)
        # TOMAMOS LAS VENTAS DE DIMES DS
            # CABEZA
        resCabezaComprobantesDimesDS = pd.read_sql(q.getCabezaComprobantesDimesDS, con=connFlxxDimesDS)
        resCabezaComprobantesDimesDS.to_sql("cabezacomprobantes_dimes_ds", con=connLocal, if_exists="replace", index=False)
            # CUERPO
        resCuerpoComprobantesDimesDS = pd.read_sql(q.getCuerpoComprobantesDimesDS, con=connFlxxDimesDS)
        resCuerpoComprobantesDimesDS.to_sql("cuerpocomprobantes_dimes_ds", con=connLocal, if_exists='replace', index=False)
        # TOMAMOS LOS DESCUENTOS DE DISTRI
        resDescuentosDistri = pd.read_sql(q.getAllDescuentosDistri, con=connFlxxDistri)
        resDescuentosDistri.to_sql("descuentos_distri", con=connLocal, if_exists='replace', index=False)
        # TOMAMOS LOS ARTICULOS DE DISTRI
        resArticulosDistri = pd.read_sql(q.getAllArticulosDistri, con=connFlxxDistri)
        resArticulosDistri.to_sql("articulos_distri", con=connLocal, if_exists='replace', index=False)
        # TOMAMOS LOS MARKUPS DE DISTRI
        resMarkups = pd.read_excel("helpers/data/informe_markups_erroneos.xlsx", engine="openpyxl")
        resMarkups = resMarkups[["CODIGOMARCA","DESCRIPCION","MARGEN"]]
        print(resMarkups)
        for i in range(len(resMarkups)):
            num = resMarkups.loc[i, "CODIGOMARCA"]
            formatted_num = num
            if num != '80':
                formatted_num = "{:03}".format(num)
            resMarkups.loc[i, "CODIGOMARCA"] = formatted_num
        resMarkups.to_sql("markups", con=connLocal, if_exists='replace', index=False)
        print("Se han actualizado las tablas locales")
        res = {"message": "Se han actualizado las tablas locales"}
    except Exception as e:
        res = {"error": str(e)}
        return res
