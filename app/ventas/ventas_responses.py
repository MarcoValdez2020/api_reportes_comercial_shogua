from pydantic import BaseModel
from datetime import date
from typing import List

# Definir el modelo de respuesta para ventas agrupadas por mes
class MonthlySalesResponse(BaseModel):
    mes: date
    whscode: str
    total_venta_neta_con_iva: float

# Lista de ventas agrupadas por mes
class SalesGroupedByMonthResponse(BaseModel):
    ventas: List[MonthlySalesResponse]