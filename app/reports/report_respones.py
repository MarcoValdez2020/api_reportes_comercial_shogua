from pydantic import BaseModel

# Definicion del modelo de ventas de cierre de mes para Ay Güey y Mumuso
class EndMonthReportAGyMumuso(BaseModel):
    nombre_tienda:str
    venta_mensual_anio_anterior: float
    venta_mensual_anio_actual: float
    variacion_mes_porcentaje: float
    variacion_mes_efectivo: float
    ytd_anio_anterior: float
    ytd_anio_actual: float
    variacion_ytd_porcentaje: float
    variacion_ytd_efectivo: float
    inventario: int
    mos: float