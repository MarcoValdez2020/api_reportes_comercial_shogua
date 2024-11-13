from pydantic import BaseModel
from datetime import date
from typing import List
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
