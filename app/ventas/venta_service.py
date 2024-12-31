import pandas as pd
import calendar
from typing import List, Tuple
from datetime import datetime, date
from fastapi import  HTTPException
from typing import List, TypeVar
from sqlmodel import SQLModel
from core.unit_of_work import UnitOfWork
from ventas.venta_schemas import Venta
from shared.shared_service import SharedService
from shared.shared_schemas import Tienda




# Definimos la calse servicio, que sera la encargada de gestionar la logica de negocio, haciendo llamadas al repositorio
class VentaService:
    def __init__(self):
        # Si necesitamos inyecciones de dependencias las pasamos en el constructor :)
        pass
        
    
    def get_all_sales_by_brand(self,nombre_marca:str, fecha_inicio:date, fecha_fin:date) -> List[Venta]:
        """Obtener todas las ventas de una marca en un rago de fechas de la base de datos."""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                return uow.venta_repository.get_all_by_brand_name(nombre_marca, fecha_inicio, fecha_fin)
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener registros")
            
    def get_sales_grouped_by_month_and_whscode(self,nombre_marca:str, fecha_inicio:date, fecha_fin:date) -> List[Tuple[str, str, float]]:
        """Obtener todas las ventas de una marca agrupadas por tienda, año mes en un rago de fechas de la base de datos."""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                return uow.venta_repository.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio, fecha_fin)
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener registros")


    def get_sales_grouped_by_year_and_whscode(self,nombre_marca:str, fecha_inicio:date, fecha_fin:date) -> List[Tuple[str, str, float]]:
        """Obtener todas las ventas de una marca agrupadas por tienda y año en un rago de fechas de la base de datos."""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                return uow.venta_repository.get_sales_grouped_by_year_and_whscode(nombre_marca, fecha_inicio, fecha_fin)
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener registros")
    

    def get_ventas_promedio_mensual_por_marca(self, nombre_marca: str,fecha_inicio:date, fecha_fin: date) -> list: 
        """Obtener las ventas promedio por tienda en un rago de fechas de la base de datos."""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                return uow.venta_repository.get_ventas_promedio_mensual_por_marca(nombre_marca, fecha_inicio, fecha_fin)
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener registros")


    #? Funciones de transformacion de datos

    def transform_sales_gropued_by_month(self, ventas_mes:List[Tuple[str, str, float]]):
            """Método para transformar de lista de tupla a lista de diccionarios"""
            result_dict = []
            
            for record in ventas_mes:
                # La tupla tiene el siguiente formato: (mes, whscode, total_venta_neta_con_iva)
                mes = record[0].strftime('%Y-%m-%d')  # Formateamos la fecha en 'yyyy-mm-dd'
                whscode = record[1]  # Código del almacén
                total_venta_neta_con_iva = record[2]  # Convertimos el total de Decimal a float
                
                # Creamos el diccionario con el formato deseado
                result_dict.append({
                    'mes': mes,
                    'whscode': whscode,
                    'total_venta_neta_con_iva': float(total_venta_neta_con_iva)
                })

            return result_dict


    def transform_sales_gropued_by_year(self, ventas_anio:List[Tuple[str, str, float]]):
            """Método para transformar de lista de tupla a lista de diccionarios"""
            result_dict = []
            
            for record in ventas_anio:
                # La tupla tiene el siguiente formato: (mes, whscode, total_venta_neta_con_iva)
                anio = record[0].strftime('%Y-%m-%d')  # Formateamos la fecha en 'yyyy-mm-dd'
                whscode = record[1]  # Código del almacén
                total_venta_neta_con_iva = record[2]  # Convertimos el total de Decimal a float
                
                # Creamos el diccionario con el formato deseado
                result_dict.append({
                    'anio': anio,
                    'whscode': whscode,
                    'total_venta_neta_con_iva': float(total_venta_neta_con_iva)
                })

            return result_dict


    def get_first_and_last_sale_date_by_brand_name(self,nombre_marca:str):
            """Obtener la primera y ultima fecha de ventas de una marca"""
            with UnitOfWork() as uow:
                # Intenta la operacion en la bd
                try:
                    fechas_df =  uow.venta_repository.get_first_and_last_sale_date_by_brand_name(nombre_marca)
                    return fechas_df.to_dict('records')
                except Exception as e:
                        # Log de la excepción para saber el error
                        print(f"Error al obtener registros: {e}")

    def getAvailableSalesYears(self, nombre_marca:str) -> List[int]:
        """Obtener los años en que una marca tiene ventas registradas"""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                data = uow.venta_repository.get_years_with_sales_by_brand_name(nombre_marca)
                # Diccionario para convertir números de mes a nombres de mes
                meses_dict = {
                    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
                    7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
                }

                # Convertir los números de mes a nombres de mes y estructurar la respuesta
                response = []
                for anio, meses in data:
                    # Convertir los números de mes flotantes a enteros y luego a nombres
                    meses_con_nombres = [{'index':int(mes),'name': meses_dict.get(int(mes))} for mes in set(meses)]
                    response.append({
                        'anio': int(anio),
                        'meses_venta': meses_con_nombres
                    })
                
                return response

            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener registros")


    #? Funciones para trabajar con datos

