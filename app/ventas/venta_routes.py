from fastapi import APIRouter, HTTPException
from datetime import date

from ventas.venta_schemas import Venta
from ventas.venta_service import VentaService
from ventas.ventas_responses import SalesGroupedByMonthResponse, MonthlySalesResponse


router = APIRouter()
ventas_service = VentaService()


@router.get("/get-all-by-brand-name",response_model=list[Venta])
async def get_all_ventas_by_brand_name(
    nombre_marca:str, 
    fecha_inicio:date, 
    fecha_fin:date
):
    try:
        ventas = ventas_service.get_all_sales_by_brand(nombre_marca, fecha_inicio, fecha_fin)
        
        return ventas
    except Exception as e:
        print(f"Error en get_all_products: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    

@router.get("/get-all-gruped-by-month-and-whscode")
async def get_sales_grouped_by_month_and_whscode(
    nombre_marca:str,
    fecha_inicio:date, 
    fecha_fin:date
):
    try:
        ventas = ventas_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio, fecha_fin)
        # Convertir a diccionario usando comprensión
        print(f'*************************** ventas  type: {type(ventas)} *********************************')
        ventas_agrupadas = ventas_service.transform_sales_data(ventas)
        # print(ventas_agrupadas)

        # print(ventas)
        print(ventas_agrupadas)
        return 'ventas'
    except Exception as e:
        print(f"Error en get_sales_grouped_by_month_and_whscode: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    

