from fastapi import APIRouter, HTTPException
from datetime import date

from ventas.venta_service import VentaService
from shared.shared_service import SharedService
from reports.report_service import ReportService
from inventarios.inventario_service import InventarioService

router = APIRouter()
shared_service = SharedService()
ventas_service = VentaService()
inventario_service = InventarioService()
report_service = ReportService(shared_service, ventas_service, inventario_service)


@router.get("/get-month-end-report-ag-y-mu")
async def get_month_end_report_ag_y_mu(
    nombre_marca:str, 
    mes:str
):
    try:
        cierre_mes = report_service.get_month_end_report_ag_y_mu(nombre_marca, mes)
        return cierre_mes
    except Exception as e:
        print(f"Error en get_month_end_report_ag_y_mu: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    