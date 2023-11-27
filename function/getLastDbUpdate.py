from helpers.connection import connectionConfig
import pandas as pd
import function.queries as  q


def getLastInfoUpdate():
    try:
        # 1. Conectamos a la base de datos local
        connConfig = connectionConfig()
        # 2. Hacemos la consulta
        res = pd.read_sql(q.getLastUpdateInfo, con=connConfig)
        # 3. Devolvemos el resultado
        if res.empty:
            return {"message": f"No hay historial de informes en la base de datos local. Crea un primer informe precionando crear informe."}
        res = res.to_dict(orient="records")[0]
        return res
    except Exception as e:
        return {"error": str(e)}
    
    