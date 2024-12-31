from fastapi import APIRouter, HTTPException
from datetime import date

from ventas.venta_schemas import Venta
from ventas.venta_service import VentaService
from ventas.ventas_responses import FirstAndLastSaleDate, MonthlySalesResponse, YearlySalesResponse, YearsWithSalesResponse

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
    

@router.get("/get-all-gruped-by-month-and-whscode",response_model=list[MonthlySalesResponse])
async def get_sales_grouped_by_month_and_whscode(
    nombre_marca:str,
    fecha_inicio:date, 
    fecha_fin:date
):
    try:
        ventas_mes = ventas_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio, fecha_fin)
        # Convertir a diccionario
        ventas_mes_dict = ventas_service.transform_sales_gropued_by_month(ventas_mes)

        return ventas_mes_dict
    except Exception as e:
        print(f"Error en get_sales_grouped_by_month_and_whscode: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    

@router.get("/get-all-gruped-by-year-and-whscode",response_model=list[YearlySalesResponse])
async def get_sales_grouped_by_year_and_whscode(
    nombre_marca:str,
    fecha_inicio:date, 
    fecha_fin:date
):
    try:
        ventas_anio = ventas_service.get_sales_grouped_by_year_and_whscode(nombre_marca, fecha_inicio, fecha_fin)
        # Convertir a diccionario
        ventas_dict = ventas_service.transform_sales_gropued_by_year(ventas_anio)
        # Retornar el diccionario
        return ventas_dict
    except Exception as e:
        print(f"Error en get_sales_grouped_by_year_and_whscode: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    


@router.get("/get-sale-dates-by-brand-name", response_model=list[FirstAndLastSaleDate])
async def get_first_and_last_sale_date_by_brand_name(
    nombre_marca:str,
):
    try:
        ultima_y_primera_fecha_venta = ventas_service.get_first_and_last_sale_date_by_brand_name(nombre_marca)
        return ultima_y_primera_fecha_venta
    except Exception as e:
        print(f"Error en get_sales_grouped_by_year_and_whscode: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    

@router.get("/get-sales-available-years-by-brand-name", response_model=list[YearsWithSalesResponse])
async def get_first_and_last_sale_date_by_brand_name(
    nombre_marca:str,
):
    try:
        anios_disponibles_ventas_marca = ventas_service.getAvailableSalesYears(nombre_marca)

        return anios_disponibles_ventas_marca
    except Exception as e:
        print(f"Error en get_sales_grouped_by_year_and_whscode: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    
