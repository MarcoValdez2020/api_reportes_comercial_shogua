from pydantic import BaseModel
from datetime import date
from typing import Dict, List
from decimal import Decimal

# Definir el modelo de respuesta para ventas agrupadas por mes-tienda
class MonthlySalesResponse(BaseModel):
    mes: date
    whscode: str
    total_venta_neta_con_iva: float

# Definicion del modelo de ventas agrupadas por año-tienda
class YearlySalesResponse(BaseModel):
    anio: date
    whscode: str
    total_venta_neta_con_iva: float

# Definicion de la respuesta del end-point de primera y ultima fecha de compra
class FirstAndLastSaleDate(BaseModel):
    marca:str
    primera_fecha_venta: date
    ultima_fecha_venta: date

class MonthSales(BaseModel):
    index: int
    name:str

class YearsWithSalesResponse(BaseModel):
    anio: int
    meses_venta: List[MonthSales]


