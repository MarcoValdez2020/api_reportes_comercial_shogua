from sqlmodel import Session,select
from productos.producto_schemas import Producto
from sqlalchemy.exc import SQLAlchemyError
from shared.shared_schemas import Marca

class ProductoRepository:
    def __init__(self, session: Session):
        self.session = session

    def add(self, producto: Producto):
        self.session.add(producto)

    def get_by_id(self, id_producto: str) -> Producto:
        statement = select(Producto).where(Producto.id_producto == id_producto)
        result = self.session.exec(statement)
        return result.first()
    
    def get_all(self) -> list[Producto]:
        statement = select(Producto)
        result = self.session.exec(statement)
        return result.all()
    
    def get_all_by_brand_name(self, nombre_marca: str) -> list[Producto]:
        statement = (
            select(Producto)
            .join(Marca)
            .where(Marca.nombre == nombre_marca)
        )

        try:
            result = self.session.exec(statement)
            return result.all()
        except SQLAlchemyError as e:
            print(f'Fallo al obtener las ventas por marca: {e}')
            return []


    def getAllProductControlsByBrandName(self, nombre_marca:str, control_name:str):
        # Crear la base de la consulta con una columna predeterminada (en este caso Producto.talla)
        if control_name == 'talla':
            column_to_select = Producto.talla
        elif control_name == 'genero':
            column_to_select = Producto.genero
        elif control_name == 'coleccion':
            column_to_select = Producto.coleccion
        elif control_name == 'disenio':
            column_to_select = Producto.disenio
        else:
            # Columna por defecto va ser talla
            control_name ='talla'
            column_to_select = Producto.talla  # o alguna otra columna válida

        # Ahora creamos la consulta con la columna seleccionada
        statement = (
            select(column_to_select)
            .distinct()
            .join(Marca)
            .where(Marca.nombre == nombre_marca)
            .where(Producto.talla.isnot(None))  # Excluye los valores NULL
        )

        try:
            result = self.session.exec(statement)
            data = result.all()

            if data != [None]:
                return data
            else:
                return []

        except SQLAlchemyError as e:
            print(f'Fallo al obtener las ventas por marca: {e}')
            return []