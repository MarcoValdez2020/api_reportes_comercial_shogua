from sqlmodel import Session,select
from productos.producto_schemas import Producto
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
            .where(Marca.name == nombre_marca)
        )
        result = self.session.exec(statement)
        return result.all()