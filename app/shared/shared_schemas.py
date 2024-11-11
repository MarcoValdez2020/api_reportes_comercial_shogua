
from typing import Optional
from sqlmodel import Field, SQLModel, Relationship
from enums import GiroMarca, TipoTienda, EstadoOperativo, Comparabilidad, SistemaERP
from productos.producto_schemas import Producto


class Marca(SQLModel, table=True):
    __tablename__ = 'marca'
    
    id_marca: Optional[int] = Field(default=None, primary_key=True)
    nombre: str = Field(..., max_length=40)  # El nombre es obligatorio
    giro: GiroMarca  = Field(..., nullable=False)


    # Relación de uno a muchos (una marca puede tener muchos productos, muchos almacenes y muchas tiendas)
    productos: list["Producto"] = Relationship(back_populates="marca")
    almacenes: list['Almacen'] = Relationship(back_populates='marca')
    tiendas: list['Tienda'] = Relationship(back_populates='marca')

class Tienda(SQLModel, table=True):
    __tablename__ = 'tienda'

    whscode: str = Field(primary_key=True)
    nombre_sucursal: str
    id_marca: int = Field(default=None, foreign_key="marca.id_marca")
    no_proscai: Optional[int] = None  # Campos opcionales
    ciudad: Optional[str] = None
    tipo_tienda: TipoTienda = Field(..., nullable=False)
    estado_operativo: EstadoOperativo = Field(..., nullable=False)
    comparabilidad: Comparabilidad = Field(..., nullable=False)
    sistema_erp: SistemaERP = Field(..., nullable=False)

    # Relación con la marca (opcional, si deseas acceder a la relación)
    marca: 'Marca' = Relationship(back_populates='tiendas')


class Almacen(SQLModel, table=True):
    __tablename__ = 'almacen'
    
    # Este es el modelo de nuestra tabla "almacen"
    id_almacen: int = Field(primary_key=True)
    nombre: str = Field(..., nullable=False, max_length=100)
    whscode: str = Field(..., nullable=False, max_length=20)  # WhereHouseCode del almacen
    id_marca: int = Field(foreign_key='marca.id_marca', nullable=False)  # Marca a la que pertenece el almacen
    
    # Relación con la marca
    marca: 'Marca' = Relationship(back_populates='almacenes')

