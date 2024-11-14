from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import date

# Modelo de la tabla Inventario_Tienda
class InventarioTienda(SQLModel, table=True):
    __tablename__ = 'inventario_tienda'
    
    id_inventario_tienda: str = Field(primary_key=True, max_length=250)  # Compuesto por whscode+id_producto
    whscode: str = Field(foreign_key="tienda.whscode", max_length=20, nullable=False)
    id_producto: str = Field(foreign_key="producto.id_producto", max_length=100, nullable=False)
    existencia: Optional[int] = None

    def to_dict(self):
        """Devuelve un diccionario del objeto."""
        return {
            'id_inventario_tienda': self.id_inventario_tienda,
            'whscode': self.whscode,
            'id_producto': self.id_producto,
            'existencia': self.existencia
        }


# Modelo de la tabla HistorialInventarioTienda
class HistorialInventarioTienda(SQLModel, table=True):
    __tablename__ = 'historial_inventario_tienda'
    
    id_historial_inventario_tienda: str = Field(primary_key=True, max_length=255)  # Compuesto por whscode+id_producto+fecha
    fecha: date
    whscode: str = Field(foreign_key="tienda.whscode", max_length=20, nullable=False)
    id_producto: str = Field(foreign_key="producto.id_producto", max_length=100, nullable=False)
    existencia: Optional[int] = None

    def to_dict(self):
        """Devuelve un diccionario del objeto."""
        return {
            'id_historial_inventario_tienda': self.id_historial_inventario_tienda,
            'fecha': self.fecha,
            'whscode': self.whscode,
            'id_producto': self.id_producto,
            'existencia': self.existencia
        }


# Modelo de la tabla Inventario_Almacen
class InventarioAlmacen(SQLModel, table=True):
    __tablename__ = 'inventario_almacen'
    
    id_inventario_almacen: str = Field(primary_key=True, max_length=250)  # Compuesto por id_almacen+id_producto
    id_almacen: int = Field(foreign_key="almacen.id_almacen", nullable=False)
    id_producto: str = Field(foreign_key="producto.id_producto", max_length=100, nullable=False)
    existencia: Optional[int] = None

    def to_dict(self):
        """Devuelve un diccionario del objeto."""
        return {
            'id_inventario_almacen': self.id_inventario_almacen,
            'id_almacen': self.id_almacen,
            'id_producto': self.id_producto,
            'existencia': self.existencia
        }


# Modelo de la tabla HistorialInventarioAlmacen
class HistorialInventarioAlmacen(SQLModel, table=True):
    __tablename__ = 'historial_inventario_almacen'
    
    id_historial_inventario_almacen: str = Field(primary_key=True, max_length=255)  # Compuesto por id_almacen+id_producto+fecha
    fecha: date
    id_almacen: int = Field(foreign_key="almacen.id_almacen", nullable=False)
    id_producto: str = Field(foreign_key="producto.id_producto", max_length=100, nullable=False)
    existencia: Optional[int] = None

    def to_dict(self):
        """Devuelve un diccionario del objeto."""
        return {
            'id_historial_inventario_almacen': self.id_historial_inventario_almacen,
            'fecha': self.fecha,
            'id_almacen': self.id_almacen,
            'id_producto': self.id_producto,
            'existencia': self.existencia
        }
