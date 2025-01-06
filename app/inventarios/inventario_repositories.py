from sqlmodel import Session,select
from sqlalchemy import text
from sqlalchemy import func
from datetime import date
from typing import List,Tuple
from inventarios.inventario_schemas import InventarioTienda, InventarioAlmacen, HistorialInventarioTienda, HistorialInventarioAlmacen
from shared.shared_schemas import Marca, Tienda, Almacen


class InventarioRepository:
    def __init__(self, session: Session):
        self.session = session

    #? Funciones para inventario de tienda

    def get_all_store_inventories(self) -> list[InventarioTienda]:
        statement = select(InventarioTienda)
        result = self.session.exec(statement)
        return result.all()
    

    def get_all_store_inventories_by_brand_name(self, nombre_marca: str) -> list[InventarioTienda]:
        statement = (
            select(InventarioTienda)
            .join(Tienda, InventarioTienda.whscode == Tienda.whscode)  # JOIN entre Venta y Producto
            .join(Marca, Marca.id_marca == Tienda.id_marca)  # JOIN entre Producto y Marca
            .where(Marca.nombre == nombre_marca) # Filtro por nombre de marca
        )
        try:
            result = self.session.exec(statement)
        except Exception as e:
            print(f'Fallo al obtener las ventas por marca: {e}')
        return result.all()
    

    # Método de repositorio para obtener los la suma total de stock por tienda
    def get_store_inventories_total_stock_by_brand_name(self, nombre_marca: str) -> List[Tuple[str, int]]:
        # Realizar la consulta utilizando SQLAlchemy (funciones como DATE_TRUNC)
        statement = (
            select(
                InventarioTienda.whscode.label('whscode'),  # Agrupación por whscode
                func.sum(InventarioTienda.existencia).label('total_existencias')
            )
            .join(Tienda, InventarioTienda.whscode == Tienda.whscode)  # JOIN entre Tienda e InventarioTienda
            .join(Marca, Marca.id_marca == Tienda.id_marca)  # JOIN entre Marca y Tienda
            .where(Marca.nombre == nombre_marca)  # Filtro por nombre de marca
            .group_by(InventarioTienda.whscode)  # Agrupamos whscode
            .order_by('whscode')  # Ordenamos por mes y whscode
        )
        try:
            result = self.session.exec(statement)
            print(result)
        except Exception as e:
            print(f'Fallo al obtener las agrupadas: {e}')
        return result.all()        
    

    #? Funciones para inventario de almacen

    # Método de repositorio para obtener los la suma total de stock de almacenes virtuales de una marca
    def get_virtual_warehouses_inventories_total_stock_by_brand_name(self, nombre_marca: str):
        # Realizar la consulta utilizando SQLAlchemy (funciones como DATE_TRUNC)
        statement = (
            select(
                func.sum(InventarioAlmacen.existencia).label('total_existencias')
            )
            .join(Almacen, InventarioAlmacen.id_almacen == Almacen.id_almacen)  # JOIN entre Tienda e InventarioTienda
            .join(Marca, Marca.id_marca == Almacen.id_marca)  # JOIN entre Marca y Tienda
            .where(
                Marca.nombre == nombre_marca,  # Filtro por nombre de marca
                ~Almacen.whscode.in_(['CDCUN01', 'CDMX01'])  # Exceptuamos los almacenes fisicos
            )
        )
        try:
            result = self.session.exec(statement)
            print(result)
        except Exception as e:
            print(f'Fallo al obtener las agrupadas: {e}')
        return result.all()  


    # Método de repositorio para obtener los la suma total de stock de almacenes fisicos de una marca
    def get_physical_warehouses_inventories_total_stock_by_brand_name(self, nombre_marca: str) -> List[Tuple[str, int]]:
        # Realizar la consulta utilizando SQLAlchemy (funciones como DATE_TRUNC)
        statement = (
            select(
                InventarioAlmacen.id_almacen.label('id_almacen'),
                Almacen.whscode.label('whscode'),
                func.sum(InventarioAlmacen.existencia).label('total_existencias')
            )
            .join(Almacen, InventarioAlmacen.id_almacen == Almacen.id_almacen)  # JOIN entre Almacen e InventarioAlmacen
            .join(Marca, Marca.id_marca == Almacen.id_marca)  # JOIN entre Marca y Almacen
            .where(
                Marca.nombre == nombre_marca,  # Filtro por nombre de marca
                Almacen.whscode.in_(['CDCUN01', 'CDMX01'])  # Filtro para almacenes físicos
            )
            .group_by(InventarioAlmacen.id_almacen, Almacen.whscode)
        )
    
        
        try:
            result = self.session.exec(statement)
            print(result)
            # Convertimos a lista; si no hay resultados, retorna una lista vacía
            return result.all() if result else []
        except Exception as e:
            print(f'Fallo al obtener las agrupadas: {e}')
            return []  



    #? Funciones para historial inventario de tienda
    def get_history_store_inventories_total_stock_by_brand_name(self, nombre_marca:str, fecha:date) -> List[Tuple[str, int]]:
        # Realizar la consulta utilizando SQLAlchemy (funciones como DATE_TRUNC)
        statement = (
            select(
                HistorialInventarioTienda.whscode.label('whscode'),  # Agrupación por whscode
                func.sum(HistorialInventarioTienda.existencia).label('total_existencias')
            )
            .join(Tienda, HistorialInventarioTienda.whscode == Tienda.whscode)  # JOIN entre Tienda e InventarioTienda
            .join(Marca, Marca.id_marca == Tienda.id_marca)  # JOIN entre Marca y Tienda
            .where(Marca.nombre == nombre_marca)  # Filtro por nombre de marca
            .where(HistorialInventarioTienda.fecha == fecha)  # Filtro por fecha
            .group_by(HistorialInventarioTienda.whscode)  # Agrupamos whscode
            .order_by('whscode')  # Ordenamos por mes y whscode
        )
        try:
            result = self.session.exec(statement)
            return result.all()        
        except Exception as e:
            print(f'Fallo al obtener las agrupadas: {e}')
            return []
    
    # Funcion para obtener una lista con las fechas de historial de inventarios de cierres de mes disponibles    
    def get_end_month_history_inventory_list(self, nombre_marca: str):
        """Funcion para obtener la lista de los inventarios de cierre de mes disponibles"""
        # SQL crudo para la consulta
        sql = """
            WITH cierres_validos AS (
                SELECT 
                    DATE_PART('year', hi.fecha - INTERVAL '1 month') AS anio, -- Asociar con el mes anterior
                    DATE_PART('month', hi.fecha - INTERVAL '1 month') AS mes -- Asociar con el mes anterior
                FROM historial_inventario_tienda hi
                JOIN tienda t ON hi.whscode = t.whscode -- Relacionamos con la tabla tienda
                JOIN marca m ON t.id_marca = m.id_marca -- Añadimos el JOIN con la tabla marca
                WHERE 
                    DATE_PART('day', hi.fecha) = 1 -- Solo considerar días 1
                    AND m.nombre = :nombre_marca -- Filtrar por el nombre de la marca
                GROUP BY DATE_PART('year', hi.fecha - INTERVAL '1 month'), DATE_PART('month', hi.fecha - INTERVAL '1 month')
            )
            SELECT 
                anio,
                ARRAY_AGG(TO_CHAR(TO_DATE(mes::TEXT, 'MM'), 'TMMonth')) AS inventarios
            FROM cierres_validos
            GROUP BY anio
            ORDER BY anio;
        """

        try:
            # Ejecutamos el SQL crudo pasando el parámetro de manera segura
            result = self.session.exec(text(sql), params={'nombre_marca': nombre_marca})
            return result.fetchall()
        except Exception as e:
            print(f'Fallo al obtener los años con inventarios de una marca: {e}')
            return []

    #? Funciones para historial inventario de almacen
    # Método de repositorio para obtener los la suma total de stock de almacenes virtuales de una marca
    def get_history_from_virtual_warehouses_inventories_total_stock_by_brand_name(self, nombre_marca: str, fecha:date):
        # Realizar la consulta utilizando SQLAlchemy (funciones como DATE_TRUNC)
        statement = (
            select(
                func.sum(HistorialInventarioAlmacen.existencia).label('total_existencias')
            )
            .join(Almacen, HistorialInventarioAlmacen.id_almacen == Almacen.id_almacen)  # JOIN entre Tienda e InventarioTienda
            .join(Marca, Marca.id_marca == Almacen.id_marca)  # JOIN entre Marca y Tienda
            .where(
                Marca.nombre == nombre_marca,  # Filtro por nombre de marca
                HistorialInventarioAlmacen.fecha == fecha, # Filtrar por fecha
                ~Almacen.whscode.in_(['CDCUN01', 'CDMX01'])  # Exceptuamos los almacenes fisicos
            )
        )
        try:
            result = self.session.exec(statement)
            return result.one()
        except Exception as e:
            print(f'Fallo al obtener las agrupadas: {e}')
            return [] 


    # Método de repositorio para obtener los la suma total de stock de almacenes fisicos de una marca
    def get_history_from_physical_warehouses_inventories_total_stock_by_brand_name(self, nombre_marca: str, fecha:date) -> List[Tuple[str, int]]:
        # Realizar la consulta utilizando SQLAlchemy (funciones como DATE_TRUNC)
        statement = (
            select(
                HistorialInventarioAlmacen.id_almacen.label('id_almacen'),
                Almacen.whscode.label('whscode'),
                func.sum(HistorialInventarioAlmacen.existencia).label('total_existencias')
            )
            .join(Almacen, HistorialInventarioAlmacen.id_almacen == Almacen.id_almacen)  # JOIN entre Almacen e HistorialInventarioAlmacen
            .join(Marca, Marca.id_marca == Almacen.id_marca)  # JOIN entre Marca y Almacen
            .where(
                Marca.nombre == nombre_marca,  # Filtro por nombre de marca
                HistorialInventarioAlmacen.fecha == fecha,  # Filtro por nombre de marca
                Almacen.whscode.in_(['CDCUN01', 'CDMX01'])  # Filtro para almacenes físicos
            )
            .group_by(HistorialInventarioAlmacen.id_almacen, Almacen.whscode)
        )
    
        
        try:
            result = self.session.exec(statement)
            print(result)
            # Convertimos a lista; si no hay resultados, retorna una lista vacía
            return result.all() if result else []
        except Exception as e:
            print(f'Fallo al obtener las agrupadas: {e}')
            return []  

