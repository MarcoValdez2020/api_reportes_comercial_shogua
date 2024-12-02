from sqlmodel import Session,select
from sqlalchemy import func
from typing import List,Tuple
from inventarios.inventario_schemas import InventarioTienda, InventarioAlmacen
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
                func.sum(InventarioAlmacen.existencia).label('total_existencias')
            )
            .join(Almacen, InventarioAlmacen.id_almacen == Almacen.id_almacen)  # JOIN entre Almacen e InventarioAlmacen
            .join(Marca, Marca.id_marca == Almacen.id_marca)  # JOIN entre Marca y Almacen
            .where(
                Marca.nombre == nombre_marca,  # Filtro por nombre de marca
                Almacen.whscode.in_(['CDCUN01', 'CDMX01'])  # Filtro para almacenes físicos
            )
            .group_by(InventarioAlmacen.id_almacen)
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

    #? Funciones para historial inventario de almacen
    
    #? Funciones para historial inventario de almacen
