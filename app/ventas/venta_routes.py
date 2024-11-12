from fastapi import APIRouter, HTTPException
from datetime import date

from ventas.venta_schemas import Venta
from ventas.venta_service import VentaService
from ventas.ventas_responses import MonthlySalesResponse


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
        ventas = ventas_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio, fecha_fin)
        # Convertir a diccionario usando comprensión
        result_dict = []
        
        for record in ventas:
            # La tupla tiene el siguiente formato: (mes, whscode, total_venta_neta_con_iva)
            mes = record[0].strftime('%Y-%m-%d')  # Formateamos la fecha en 'yyyy-mm-dd'
            whscode = record[1]  # Código del almacén
            nombre_tienda = record[2]
            total_venta_neta_con_iva = record[3]  # Convertimos el total de Decimal a float
            
            # Creamos el diccionario con el formato deseado
            result_dict.append({
                'mes': mes,
                'whscode': whscode,
                'nombre_tienda': nombre_tienda,
                'total_venta_neta_con_iva': float(total_venta_neta_con_iva)
            })

        return result_dict
    except Exception as e:
        print(f"Error en get_sales_grouped_by_month_and_whscode: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    

