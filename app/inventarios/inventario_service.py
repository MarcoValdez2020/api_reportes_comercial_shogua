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
class InventarioService:
    def __init__(self):
        # Si necesitamos inyecciones de dependencias las pasamos en el constructor :)
        pass

    def get_store_inventories_total_stock_by_brand_name(self, nombre_marca: str):
        """Obtener todas las ventas de una marca en un rago de fechas de la base de datos."""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                return uow.inventario_repository.get_store_inventories_total_stock_by_brand_name(nombre_marca)
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener registros")
            


    def transform_store_inventories_total_stock(self, inventarios_tiendas:List[Tuple[str, int]]):
            """Método para transformar de lista de tupla a lista de diccionarios"""
            result_dict = []
            
            for record in inventarios_tiendas:
                # La tupla tiene el siguiente formato: (mes, whscode, total_venta_neta_con_iva)
                whscode = record[0]  # Código del almacén
                existencia = record[1] 
                
                # Creamos el diccionario con el formato deseado
                result_dict.append({
                    'whscode': whscode,
                    'existencia': int(existencia)
                })

            return result_dict    


    # Funcion para obtener la cantidad de stock actual de todos los inventarios fisicos
    def get_virtual_warehouses_inventories_total_stock_by_brand_name(self, nombre_marca: str):
        """Función para obtener El total de existencias en cantidad de los almacenes virtuales de una marca"""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                return uow.inventario_repository.get_virtual_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")



    # Funcion para obtener los almacenes fisicos con su cantidad
    def get_physical_warehouses_inventories_total_stock_by_brand_name(self, nombre_marca: str):
        """Función para obtener El total de existencias en cantidad de los almacenes virtuales de una marca"""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                return uow.inventario_repository.get_physical_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")

    # Funcion para obtener el inventario de una marca del historial de inventarios
    def get_store_inventories_total_stock_by_brand_name_and_month(self, nombre_marca:str, mes:str, anio:int):
        """Función para obtener El total de existencias en cantidad de los almacenes virtuales de una marca"""
        # Convertimos el mes a número a partir de un diccionario de meses con el nombre de los meses en español mexico
        meses_dict = {
                'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
                'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
                'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
            }
        
        mes = mes.lower()
        mes_numero = meses_dict[mes]
        # Calculamos el mes siguiente, pero si es diciembre, el siguiente es enero
        if mes_numero == 12:
            # Si el mes supera al 12, se reinicia el mes a 1 y se incrementa el año
            mes_siguiente = 1
            anio += 1
        else:
            mes_siguiente = mes_numero + 1

        # Creamos una fecha a partir del mes siguiente y año recibido
        fecha = date(anio, mes_siguiente, 1)

        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                print(f'trayendo el inventario {fecha}')
                return uow.inventario_repository.get_history_store_inventories_total_stock_by_brand_name(nombre_marca, fecha)
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")

    # Funcion para obtener los años con inventarios de cierre de mes disponibles
    def get_end_month_history_inventory_list(self, nombre_marca:str):
        """Funcion para obtener la lista de los inventarios de cierre de mes disponibles"""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                # Diccionario para convertir números de mes a nombres de mes
                meses_dict = {
                    'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4, 'Mayo': 5, 'Junio': 6,
                    'Julio': 7, 'Agosto': 8, 'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12
                }
                response = []
                data = uow.inventario_repository.get_end_month_history_inventory_list(nombre_marca)
                for anio, meses in data:
                    # Convertimos los nombres de meses a índices y preparamos la lista
                    meses_con_nombres = [
                        {'index': meses_dict[mes], 'name': mes.lower()}  # Convertimos a minúsculas
                        for mes in sorted(meses, key=lambda m: meses_dict[m])  # Ordenamos por índice
                    ]
                    
                    response.append({
                        'anio': int(anio),  # Convertimos el año a entero
                        'inventario_cierre': meses_con_nombres
                    })
                
                return response
                # Convertir los resultados a un DataFrame de Pandas                
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")