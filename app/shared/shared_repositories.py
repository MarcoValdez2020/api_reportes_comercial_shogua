from sqlmodel import Session,select
from datetime import date
from sqlalchemy import func
from typing import List,Tuple

from shared.shared_schemas import Marca, Tienda, Almacen, PresupuestoTienda


class SharedRepository:
    def __init__(self, session: Session):
        self.session = session

    #? Funciones de las marcas
    def get_all_brands(self) -> list[Marca]:
        statement = select(Marca)
        result = self.session.exec(statement)
        return result.all()

    #? Funciones de las tiendas
    def get_all_stores(self) -> list[Tienda]:
        statement = select(Tienda)
        result = self.session.exec(statement)
        return result.all()
    
    def get_all_stores_by_brand_name(self, nombre_marca: str) -> list[Tienda]:
        statement = (
            select(Tienda)
            .join(Marca, Marca.id_marca == Tienda.id_marca)  # JOIN entre Tienda y Marca
            .where(Marca.nombre == nombre_marca) # Filtro por nombre de marca
        )
        try:
            result = self.session.exec(statement)
        except Exception as e:
            print(f'Fallo al obtener las ventas por marca: {e}')
        return result.all()
    
    
    #? Funciones de los almacenes

    #? Funciones de los presupuestos de tienda
    def get_all_store_budgets(self) -> list[PresupuestoTienda]:
        statement = select(PresupuestoTienda)
        result = self.session.exec(statement)
        return result.all()