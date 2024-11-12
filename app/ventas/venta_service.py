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
    def __init__(self, shared_service: SharedService):
        self.shared_service = shared_service
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
    

    def get_month_end_report_ag_y_mu(self,nombre_marca:str, mes:str):
        """Metodo para obtener los datos de cierre de mes de las marcas Ay Güey y Mumuso"""

        # Obtener registros de las tiendas por marca y transformalo en df
        tiendas = [t.to_dict() for t in self.shared_service.get_all_stores_by_brand_name(nombre_marca)]

        tiendas_df = pd.DataFrame(tiendas)
        print(tiendas_df.info())
        
        # Obtener todas las ventas de inicio a fin del mes seleccionado
        anio_actual = date.today().year

        anio_anterior = date.today().year-1
        meses_dict = {
                'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
                'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
                'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
            }
        mes = mes.lower()
        mes_numero = meses_dict[mes]
        _,ultimo_dia_mes = calendar.monthrange(anio_actual,mes_numero)
        
        fecha_inicio = date(anio_actual,mes_numero,1)
        print(fecha_inicio)
        fecha_fin = date(anio_actual,mes_numero,ultimo_dia_mes)
        print(fecha_fin)

        ventas_mes= self.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio, fecha_fin)
        ventas_mes_dict = self.transform_sales_gropued_by_month(ventas_mes)
        ventas_mes_df = pd.DataFrame(ventas_mes_dict)
        print(ventas_mes_df.info())
        print(ventas_mes_df)

        ventas_mes_prot = pd.merge(ventas_mes_df,tiendas_df[['whscode','nombre_sucursal']],on='whscode',how='left')

        print(ventas_mes_prot)

        # Obtener todas las ventas de inicio de año hasta el fin del mes seleccionado


        # Obtener
        
        
        return ventas_mes_prot



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

    def convertir_a_lista_diccionarios(self,tuplas: List[Tuple[str, str, float]]):
        # Convertir la lista de tuplas a una lista de diccionarios con las claves deseadas
        lista_diccionarios = [
            {
                'anio': t[0],
                'whscode': t[1],
                'total_venta_neta_con_iva': t[2]
            }
            for t in tuplas
        ]
        return lista_diccionarios

    #? Funciones para trabajar con datos

