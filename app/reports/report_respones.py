from pydantic import BaseModel
from typing import List, Optional
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
        venta_objetivo: Optional[float] = 0
        variacion_vta_obj_porcentaje: Optional[float] = 0
        punto_eq: Optional[float] = 0
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
        total_venta_objetivo: float
        total_variacion_vta_obj_porcentaje: float
        total_punto_eq: float
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
        existencia_tienda: int
        mos_tienda: float
        meses_con_venta: int

        # Atributos de almacen fisico (opcionales)
        existencias_almacen: Optional[float] = None
        mos_almacen: Optional[float] = None

        # Otros atributos de la tienda
        tipo_tienda: TipoTienda
        estado_operativo: EstadoOperativo
        comparabilidad: str
        ciudad: str
        total_ventas: int
        venta_promedio: float


# Clase para almacenar la respuesta final del reporte de tous
class FinallyEndMonthReportTous(BaseModel):
        stores:List[StoresEndMonthReportTous]
        existencias_almacen_virtual:float
        
        total_venta_mensual_anio_anterior_iva:float
        total_venta_mensual_anio_actual_iva: float
        total_variacion_mes_porcentaje: float
        total_variacion_mes_efectivo:float
        total_ytd_anio_anterior_iva: float
        total_ytd_anio_actual_iva: float
        total_variacion_ytd_porcentaje: float
        total_variacion_ytd_efectivo:float
        total_existencia_tiendas: int
        total_mos_tiendas: float

        total_mos_bodegas: float  # Las bodegas son almacenes físicos
        total_mos_almacenes: float # Los almacenes son almacenes virtuales


# Definicion de los modelos de respuesta de PENGUIN
class StoresEndMonthReportPenguin(BaseModel):
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
        existencia_tienda: int
        mos_tienda: float
        meses_con_venta: int

        # Otros atributos de la tienda
        tipo_tienda: TipoTienda
        estado_operativo: EstadoOperativo
        comparabilidad: str
        ciudad: str
        total_ventas: int
        venta_promedio: float


# Clase para almacenar la respuesta final del reporte de penguin
class FinallyEndMonthReportPenguin(BaseModel):
        stores:List[StoresEndMonthReportPenguin]
        existencias_almacen_virtual:float
        
        total_venta_mensual_anio_anterior_iva:float
        total_venta_mensual_anio_actual_iva: float
        total_variacion_mes_porcentaje: float
        total_variacion_mes_efectivo:float
        total_ytd_anio_anterior_iva: float
        total_ytd_anio_actual_iva: float
        total_variacion_ytd_porcentaje: float
        total_variacion_ytd_efectivo:float
        total_existencia_tiendas: int
        total_mos_tiendas: float

        total_mos_almacenes: float # Los almacenes son almacenes virtuales