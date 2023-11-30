getAllMarcas = "SELECT m.* FROM marcas m where m.muestraweb = 1"
getAllClientes = "SELECT c.* FROM clientes c where c.activo = 1"
getAllVentasMax6Meses = """SELECT x.* FROM ventas x WHERE x.fecha >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH) AND x.fecha <= CURDATE()"""
getAllDescuentos = """SELECT x.* FROM descuentos_clientes_marcas x"""

getBonificacionGeneral = """SELECT cd.codigo_particular, m.marca_nombre, x.bonificacion, y.descuento, z.ventas
                            FROM clientes_distri cd
                            JOIN marcas m ON cd.codigo_particular = m.cliente
                            JOIN ventas x ON cd.codigo_particular = x.cliente AND m.marca = x.marca
                            JOIN descuentos_clientes_marcas dcm ON cd.codigo_particular = v.cliente AND x.marca = y.marca
                            JOIN ventas z ON x.cliente = z.cliente AND x.marca = z.marca
                            group by x.cliente, x.marca
                            ORDER BY x.cliente, x.marca
                            LIMIT 10"""


getClienteInfo = """

    SELECT cd.codigo_particular, m.marca_nombre, c.bonificacion, d.descuento
    FROM clientes_distri cd
    JOIN ventas_distri v ON cd.codigo_particular = v.info_cliente
    JOIN marcas_distri m ON v.info_marca = m.marca
    JOIN descuentos_distri d ON cd.codigo_particular = d.info_cliente AND m.marca = d.info_marca
    where cd.codigo_particular = '05451'

"""

# SELECT cd.CODIGOPARTICULAR, cd.RAZONSOCIAL,md.CODIGOMARCA, cd.codigocliente, cd.bonificacion, md.DESCRIPCION,  cdp.LINEA, replace(sum(cdp.PRECIOTOTAL), '.', ',') AS 'VENTAS_{period}', replace(sum(cdp.COSTOVENTA), '.', ',') AS 'COSTO_{period}' FROM {body} cdp 
# join {head} cdp2 on cdp2.NUMEROCOMPROBANTE = cdp.NUMEROCOMPROBANTE -- Metemos cdp2 para sacar info cliente
# join clientes_distri cd on cdp2.CODIGOCLIENTE = cd.CODIGOCLIENTE -- metemos cd para sacar CODIGOPARTICULAR del cliente
# LEFT join marcas_distri md on cdp.LINEA = md.CODIGOMARCA
# where cdp2.FECHACOMPROBANTE BETWEEN '{from_date}' and '{to_date}' and cd.CODIGOPARTICULAR = '05451'
# GROUP BY cdp.LINEA, cd.CODIGOPARTICULAR
# ORDER BY  cd.CODIGOPARTICULAR, md.DESCRIPCION;
def getDataPrevia(head, body, period = "todo", from_date = "2023-01-01 00:00:00", to_date = "2023-09-05 23:59:59"): 
     return f"""
            SELECT cd.CODIGOPARTICULAR, cd.RAZONSOCIAL, cdp.linea, md.CODIGOMARCA, cd.codigocliente, replace(cd.bonificacion, ".",",") as 'BONIFICACION', md.DESCRIPCION, replace((sum(cdp.PRECIOTOTAL)/1000), '.', ',') AS 'VENTAS_{period}', replace((sum(cdp.COSTOVENTA)/1000), '.', ',') AS 'COSTO_{period}' FROM {body} cdp 
            join {head} cdp2 on cdp2.NUMEROCOMPROBANTE = cdp.NUMEROCOMPROBANTE -- Metemos cdp2 para sacar info cliente
            join clientes_distri cd on cdp2.CODIGOCLIENTE = cd.CODIGOCLIENTE -- metemos cd para sacar CODIGOPARTICULAR del cliente
            join articulos_distri ad on cdp.CODIGOPARTICULAR = ad.CODIGOPARTICULAR
            LEFT join marcas_distri md on ad.CODIGOMARCA = md.CODIGOMARCA
            where cdp2.FECHACOMPROBANTE BETWEEN '{from_date}' and '{to_date}' and cd.CODIGOPARTICULAR IN ('05451','06825')
            GROUP BY ad.CODIGOMARCA, cd.CODIGOPARTICULAR
            ORDER BY  cd.CODIGOPARTICULAR, md.DESCRIPCION;
            """

def getDataPreviaCant(head, body, datesQuery, period='todo', from_date= "2023-01-01 00:00:00", to_date = "2023-09-05 23:59:59"):
     return f"""
            SELECT cd.CODIGOPARTICULAR, cd.RAZONSOCIAL, cdp.linea, md.CODIGOMARCA, cd.codigocliente, replace(cd.bonificacion, ".",",") as 'BONIFICACION', md.DESCRIPCION, {datesQuery} FROM {body} cdp 
            join {head} cdp2 on cdp2.NUMEROCOMPROBANTE = cdp.NUMEROCOMPROBANTE -- Metemos cdp2 para sacar info cliente
            join clientes_distri cd on cdp2.CODIGOCLIENTE = cd.CODIGOCLIENTE -- metemos cd para sacar CODIGOPARTICULAR del cliente
            join articulos_distri ad on cdp.CODIGOPARTICULAR = ad.CODIGOPARTICULAR
            LEFT join marcas_distri md on ad.CODIGOMARCA = md.CODIGOMARCA
            where cdp2.FECHACOMPROBANTE BETWEEN '{from_date}' and '{to_date}' and cd.ACTIVO = 1
            GROUP BY ad.CODIGOMARCA, cd.CODIGOPARTICULAR
            ORDER BY  cd.CODIGOPARTICULAR, md.DESCRIPCION;
        """


getAllDescuentosLocal = """SELECT * FROM descuentos_distri"""


getMarkupsNOUSAR = """
    SELECT COUNT(a.CODIGOPARTICULAR) AS CANTIDAD, m.DESCRIPCION, a.MARGEN1, a.CODIGOMARCA
    FROM articulos_distri a
    JOIN marcas_distri m ON m.CODIGOMARCA = a.CODIGOMARCA
    GROUP BY a.MARGEN1, a.CODIGOMARCA, m.DESCRIPCION
"""

getMarkups = """
    SELECT m.DESCRIPCION, m.MARGEN, m.CODIGOMARCA
    FROM markups m 
"""




# bases de datos de flxx
# DISTRI
    # CLIENTES
getAllClientesDistri = "SELECT * FROM clientes c where c.activo = 1" # ✅
    # MARCAS
getAllMarcasDistri = "SELECT * FROM marcas m where m.muestraweb = 1" # ✅
    # DESCUENTOS
getAllDescuentosDistri = """SELECT * FROM DESCUENTOCLIENTESMARCAS d """ # ✅
    # ARTICULOS
getAllArticulosDistri = """SELECT * FROM ARTICULOS a WHERE a.ACTIVO = 1""" # ✅
# VENTAS DE DISTRI PPAL
start_date = "2023-01-01 00:00:00"

getAllCabezaComprobantesDistriPPAL = f"""SELECT c.TIPOCOMPROBANTE, c.NUMEROCOMPROBANTE, c.CODIGOCLIENTE, c.FECHACOMPROBANTE, c.PORCIVA1, c.iva1, c.TOTAL, c.pagado, c.TIPOIVA, c.COMPRA, c.EXENTO  FROM CABEZACOMPROBANTES c WHERE  c.FECHACOMPROBANTE BETWEEN '{start_date}' AND CURRENT_TIMESTAMP AND c.TIPOCOMPROBANTE IN ('FA', 'FB', 'NCA', 'NCB')"""
getAllCuerpoComprobantesDistriPPAL = f"""SELECT c.TIPOCOMPROBANTE, c.NUMEROCOMPROBANTE, c.LINEA, c.CODIGOARTICULO, c.PRECIOTOTAL, c.FECHAMODIFICACION, c.CODIGOPARTICULAR, c.CANTIDAD, c.costoventa FROM cuerpocomprobantes c WHERE c.FECHAMODIFICACION BETWEEN '{start_date}' AND CURRENT_TIMESTAMP AND c.TIPOCOMPROBANTE IN ('FA', 'FB', 'NCA', 'NCB')"""

# VENTAS DE DISTRI DS
getCabezaComprobantesDistriDS = f"""SELECT c.TIPOCOMPROBANTE, c.NUMEROCOMPROBANTE, c.CODIGOCLIENTE, c.FECHACOMPROBANTE, c.PORCIVA1, c.iva1, c.TOTAL, c.pagado, c.TIPOIVA, c.COMPRA, c.EXENTO  FROM CABEZACOMPROBANTES c WHERE  c.FECHACOMPROBANTE BETWEEN '{start_date}' AND CURRENT_TIMESTAMP AND c.TIPOCOMPROBANTE IN ('FA', 'FB', 'NCA', 'NCB')"""
getCuerpoComprobantesDistriDS = f"""SELECT c.TIPOCOMPROBANTE, c.NUMEROCOMPROBANTE, c.LINEA, c.CODIGOARTICULO, c.PRECIOTOTAL, c.FECHAMODIFICACION,  c.CODIGOPARTICULAR, c.CANTIDAD, c.costoventa FROM cuerpocomprobantes c WHERE c.FECHAMODIFICACION BETWEEN '{start_date}' AND CURRENT_TIMESTAMP AND c.TIPOCOMPROBANTE IN ('FA', 'FB', 'NCA', 'NCB')"""

# VENTAS DE DIMES PPAL
getCabezaComprobantesDimesPPAL = f"""SELECT c.TIPOCOMPROBANTE, c.NUMEROCOMPROBANTE, c.CODIGOCLIENTE, c.FECHACOMPROBANTE, c.PORCIVA1, c.iva1, c.TOTAL, c.pagado, c.TIPOIVA, c.COMPRA, c.EXENTO  FROM CABEZACOMPROBANTES c WHERE  c.FECHACOMPROBANTE BETWEEN '{start_date}' AND CURRENT_TIMESTAMP AND c.TIPOCOMPROBANTE IN ('FA', 'FB', 'NCA', 'NCB')"""
getCuerpoComprobantesDimesPPAL = f"""SELECT c.TIPOCOMPROBANTE, c.NUMEROCOMPROBANTE, c.LINEA, c.CODIGOARTICULO, c.PRECIOTOTAL, c.FECHAMODIFICACION,  c.CODIGOPARTICULAR, c.CANTIDAD,  c.costoventa FROM cuerpocomprobantes c WHERE c.FECHAMODIFICACION BETWEEN '{start_date}' AND CURRENT_TIMESTAMP AND c.TIPOCOMPROBANTE IN ('FA', 'FB', 'NCA', 'NCB')"""

# VENTAS DE DIMES DS
getCabezaComprobantesDimesDS = f"""SELECT c.TIPOCOMPROBANTE, c.NUMEROCOMPROBANTE, c.CODIGOCLIENTE, c.FECHACOMPROBANTE, c.PORCIVA1, c.iva1, c.TOTAL, c.pagado, c.TIPOIVA, c.COMPRA, c.EXENTO  FROM CABEZACOMPROBANTES c WHERE  c.FECHACOMPROBANTE BETWEEN '{start_date}' AND CURRENT_TIMESTAMP AND c.TIPOCOMPROBANTE IN ('FA', 'FB', 'NCA', 'NCB')"""
getCuerpoComprobantesDimesDS = f"""SELECT c.TIPOCOMPROBANTE, c.NUMEROCOMPROBANTE, c.LINEA, c.CODIGOARTICULO, c.PRECIOTOTAL, c.FECHAMODIFICACION, c.CODIGOPARTICULAR, c.CANTIDAD, c.costoventa FROM cuerpocomprobantes c WHERE c.FECHAMODIFICACION BETWEEN '{start_date}' AND CURRENT_TIMESTAMP AND c.TIPOCOMPROBANTE IN ('FA', 'FB', 'NCA', 'NCB')"""

# queries local

def getAllBrandsDistri(brands):
    print(len(brands))
    if len(brands) > 0:
        return f"""SELECT * FROM marcas_distri WHERE CODIGOMARCA IN ({brands})"""
    else:
        return f"""SELECT * FROM marcas_distri"""
getLastUpdateInfo = """SELECT * FROM info_updates ORDER BY created_at DESC LIMIT 1"""

# VPS 69
getCampamas = "SELECT * FROM CAMPANAS"

def getCreatedCampanas(date): 
    return f"""SELECT * FROM CAMPANAS WHERE created_at LIKE '%{date}%'"""

