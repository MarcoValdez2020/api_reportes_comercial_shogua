from pydantic import BaseModel
from typing import List
from shared.enums import TipoTienda, EstadoOperativo

# Definicion del modelo de ventas de cierre de mes para Ay Güey y Mumuso
class EndMonthReportAGyMumuso(BaseModel):
        whscode: str
        nombre_sucursal: str
        venta_mensual_anio_anterior_iva:float
        venta_mensual_anio_actual_iva: float
        variacion_mes_porcentaje: float
        variacion_mes_efectivo:float
        ytd_anio_anterior_iva: float
        ytd_anio_actual_iva: float
        variacion_ytd_porcentaje: float
        variacion_ytd_efectivo:float
        existencia: int
        mos: float

        meses_con_venta: int
        total_ventas: int
        venta_promedio: float
        tipo_tienda: TipoTienda
        estado_operativo: EstadoOperativo
        comparabilidad: str
        ciudad: str

class FinallyEndMonthReportAGyMumuso(BaseModel):
        stores:List[EndMonthReportAGyMumuso]
        total_venta_mensual_anio_anterior_iva:float
        total_venta_mensual_anio_actual_iva: float
        total_variacion_mes_porcentaje: float
        total_variacion_mes_efectivo:float
        total_ytd_anio_anterior_iva: float
        total_ytd_anio_actual_iva: float
        total_variacion_ytd_porcentaje: float
        total_variacion_ytd_efectivo:float
        total_existencia: int
        total_mos: float



# Definicion de los modelos de respuesta de TOUS
class StoresEndMonthReportTous(BaseModel):
        whscode: str
        nombre_sucursal: str
        venta_mensual_anio_anterior_iva:float
        venta_mensual_anio_actual_iva: float
        variacion_mes_porcentaje: float
        variacion_mes_efectivo:float
        ytd_anio_anterior_iva: float
        ytd_anio_actual_iva: float
        variacion_ytd_porcentaje: float
        variacion_ytd_efectivo:float
        existencia: int
        mos: float

        meses_con_venta: int
        total_ventas: int
        venta_promedio: float
        tipo_tienda: TipoTienda
        estado_operativo: EstadoOperativo
        comparabilidad: str
        ciudad: str

# Calse para los almacenes virtuales
class VirtualWarehouses(BaseModel):
        existencias: float

# Clase para los almacenes virtuales
class PhysicalWarehouses(BaseModel):
        nombre:str
        existencias:float
        mos: float

# Clase para almacenar la respuesta final del reporte de tous
class FinallyEndMonthReportTous(BaseModel):
        stores:List[StoresEndMonthReportTous]
        vitual_warehouses:VirtualWarehouses
        physical_warehouses: List[PhysicalWarehouses]

        total_venta_mensual_anio_anterior_iva:float
        total_venta_mensual_anio_actual_iva: float
        total_variacion_mes_porcentaje: float
        total_variacion_mes_efectivo:float
        total_ytd_anio_anterior_iva: float
        total_ytd_anio_actual_iva: float
        total_variacion_ytd_porcentaje: float
        total_variacion_ytd_efectivo:float
        total_existencia: int
        total_mos_tiendas: float
