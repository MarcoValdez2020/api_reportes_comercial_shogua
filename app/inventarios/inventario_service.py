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