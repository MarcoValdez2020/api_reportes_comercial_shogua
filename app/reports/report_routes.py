from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from datetime import date
from typing import List, Optional

import traceback


from ventas.venta_service import VentaService
from shared.shared_service import SharedService
from reports.report_service import ReportService
from inventarios.inventario_service import InventarioService

from reports.report_respones import FinallyEndMonthReportAGyMumuso
from reports.report_respones import FinallyEndMonthReportTous, FinallyEndMonthReportPenguin

router = APIRouter()
shared_service = SharedService()
ventas_service = VentaService()
inventario_service = InventarioService()
report_service = ReportService(shared_service, ventas_service, inventario_service)


# Ruta expuesta para el reporte de cierre de mes de AG y mumuso
@router.get("/get-month-end-report-ag-y-mu", response_model=FinallyEndMonthReportAGyMumuso)
async def get_month_end_report_ag_y_mu(
    nombre_marca:str, 
    mes:str,
    anio:int,
    tipo_inventario:str
):
    try:
        cierre_mes = report_service.get_month_end_report_ag_y_mu(nombre_marca, mes, anio, tipo_inventario)
        return cierre_mes
    except Exception as e:
        print(f"Error en get_month_end_report_ag_y_mu: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    

# Ruta expuesta para el reporte de cierre de mes de Tous
@router.get("/get-month-end-report-tous", response_model=FinallyEndMonthReportTous)
async def get_month_end_report_tous_hm_y_ricodeli(
    nombre_marca:str, 
    mes:str,
    anio:int,
    tipo_inventario:str
):
    try:
        cierre_mes = report_service.get_month_end_report_tous_hm_y_ricodeli(nombre_marca, mes, anio,tipo_inventario)
        return cierre_mes
    
    except ValueError as e:  # Excepción específica de tu lógica de negocio
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        tb = traceback.format_exc()  # Obtiene el traceback completo
        print(f"Error en get_month_end_report_tumi: {e}\n{tb}")
        print(f"Error en get_month_end_report_tous_hm_y_ricodeli: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    

# Ruta expuesta para el reporte de cierre de mes de Tumi
@router.get("/get-month-end-report-tumi", response_model=FinallyEndMonthReportTous)
async def get_month_end_report_tumi(
    nombre_marca:str, 
    mes:str,
    anio:int,
    tipo_inventario:str
):
    try:
        cierre_mes = report_service.get_month_end_report_tumi(nombre_marca, mes, anio, tipo_inventario)
        return cierre_mes
    
    except ValueError as e:  # Excepción específica de tu lógica de negocio
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print(f"Error en get_month_end_report_tumi: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# Ruta expuesta para el reporte de cierre de mes de UNODE50
@router.get("/get-month-end-report-unode50", response_model=FinallyEndMonthReportTous)
async def get_month_end_report_unode50(
    nombre_marca:str, 
    mes:str,
    anio: int,
    tipo_inventario:str
):
    try:
        cierre_mes = report_service.get_month_end_report_unode50(nombre_marca, mes, anio, tipo_inventario)
        return cierre_mes
    
    except ValueError as e:  # Excepción específica de tu lógica de negocio
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print(f"Error en get_month_end_report_unode50: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# Ruta expuesta para el reporte de cierre de mes de UNODE50
@router.get("/get-month-end-report-penguin", response_model=FinallyEndMonthReportPenguin)
async def get_month_end_report_penguin(
    nombre_marca:str, 
    mes:str,
    anio: int,
    tipo_inventario:str
):
    try:
        cierre_mes = report_service.get_month_end_report_penguin(nombre_marca, mes, anio,tipo_inventario)
        return cierre_mes
    
    except ValueError as e:  # Excepción específica de tu lógica de negocio
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print(f"Error en get_month_end_report_penguin: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    

class StoreDetailBodyList(BaseModel):
    whscodes: List[str]
    tallas : Optional[List[str]] = None
    generos : Optional[List[str]] = None
    colecciones : Optional[List[str]] = None
    disenios : Optional[List[str]] = None

@router.post("/get-store-detail-report")
async def get_store_detail_report(
    nombre_marca: str,
    mes: str,
    anio: int,
    tipo_inventario: str,
    storeBody: StoreDetailBodyList  # Cuerpo JSON esperado
):
    try:
        # Extraer la lista de whscodes
        whscodes_list = storeBody.whscodes
        # Extraer la lista de tallas
        tallas_list = storeBody.tallas
        # Extraer la lista de generos
        generos_list = storeBody.generos
        # Extraer la lista de colecciones
        colecciones_list = storeBody.colecciones
        # Extraer la lista de diseños
        disenios_list = storeBody.disenios


        # Lógica del servicio
        detalle_tienda = report_service.obtener_reporte_detalle_tienda(
            nombre_marca, mes, anio, tipo_inventario, whscodes_list, tallas_list, generos_list, colecciones_list, disenios_list
        )
        return detalle_tienda

    except ValueError as e:  # Excepción específica de tu lógica de negocio
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print(f"Error en get_store_detail_report: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    

@router.post("/get-store-detail-fields-combinations")
async def filtrar_campos_detalle_tienda(
    mes: str,
    anio: int,
    storeBody: StoreDetailBodyList  # Cuerpo JSON esperado
):
    try:
        # Extraer la lista de whscodes
        whscodes_list = storeBody.whscodes
        # Extraer la lista de tallas
        tallas_list = storeBody.tallas
        # Extraer la lista de generos
        generos_list = storeBody.generos
        # Extraer la lista de colecciones
        colecciones_list = storeBody.colecciones
        # Extraer la lista de diseños
        disenios_list = storeBody.disenios


        # Lógica del servicio
        combinaciones = report_service.filtrar_campos_detalle_tienda(
            whscodes_list, mes, anio, tallas_list, generos_list,  disenios_list, colecciones_list
        )
        return combinaciones

    except ValueError as e:  # Excepción específica de tu lógica de negocio
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        print(f"Error en get_store_detail_report: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")