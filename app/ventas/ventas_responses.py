from pydantic import BaseModel
from datetime import date
from typing import List
from decimal import Decimal

# Definir el modelo de respuesta para ventas agrupadas por mes
class MonthlySalesResponse(BaseModel):
    mes: date
    whscode: str
    nombre_tienda: str
    total_venta_neta_con_iva: float
