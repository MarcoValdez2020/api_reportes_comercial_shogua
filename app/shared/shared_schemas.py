
from typing import Optional
from sqlmodel import Field, SQLModel, Relationship
from shared.enums import GiroMarca, TipoTienda, EstadoOperativo, Comparabilidad, SistemaERP
from productos.producto_schemas import Producto


class Marca(SQLModel, table=True):
    __tablename__ = 'marca'
    
    id_marca: Optional[int] = Field(default=None, primary_key=True)
    nombre: str = Field(..., max_length=40)  # El nombre es obligatorio
    giro: GiroMarca  = Field(..., nullable=False)


    # Relación de uno a muchos (una marca puede tener muchos productos, muchos almacenes y muchas tiendas)
    #productos: list["Producto"] = Relationship(back_populates="marca")
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
    comparabilidad: str = Field(..., nullable=False) # No se uso enum porque se tiene error al cargarlo por el espacio en blanco
    sistema_erp: SistemaERP = Field(..., nullable=False)

    # Relación con la marca (opcional, si deseas acceder a la relación)
    marca: 'Marca' = Relationship(back_populates='tiendas')

    def to_dict(self):
        """Devuelve un diccionario del objeto."""
        return {
            'whscode':self.whscode,
            'nombre_sucursal': self.nombre_sucursal,
            'id_marca': self.id_marca,
            'no_proscai': self.no_proscai,
            'ciudad': self.ciudad,
            'tipo_tienda': self.tipo_tienda,
            'estado_operativo': self.estado_operativo,
            'comparabilidad': self.comparabilidad, 
            'sistema_erp': self.sistema_erp
        }  


class Almacen(SQLModel, table=True):
    __tablename__ = 'almacen'
    
    # Este es el modelo de nuestra tabla "almacen"
    id_almacen: int = Field(primary_key=True)
    nombre: str = Field(..., nullable=False, max_length=100)
    whscode: str = Field(..., nullable=False, max_length=20)  # WhereHouseCode del almacen
    id_marca: int = Field(foreign_key='marca.id_marca', nullable=False)  # Marca a la que pertenece el almacen
    
    # Relación con la marca
    marca: 'Marca' = Relationship(back_populates='almacenes')

