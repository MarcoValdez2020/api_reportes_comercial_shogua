from sqlmodel import SQLModel, Field, Relationship
from typing import Optional
from datetime import date
from decimal import Decimal

#from shared.shared_schemas import Marca


class Producto(SQLModel, table=True):
    __tablename__ = 'producto'
    
    # Campos
    id_producto: str = Field(default=None, primary_key=True, max_length=100)
    id_marca: int = Field(foreign_key='marca.id_marca')
    descripcion: Optional[str] = Field(default=None, max_length=300)
    talla: Optional[str] = Field(default=None, max_length=40)
    departamento: Optional[str] = Field(default=None, max_length=40)
    genero: Optional[str] = Field(default=None, max_length=40)
    categoria: Optional[str] = Field(default=None, max_length=40)
    subcategoria: Optional[str] = Field(default=None, max_length=40)
    color: Optional[str] = Field(default=None, max_length=40)
    disenio: Optional[str] = Field(default=None, max_length=100)
    coleccion: Optional[str] = Field(default=None, max_length=40)
    modelo: Optional[str] = Field(default=None, max_length=60)
    material: Optional[str] = Field(default=None, max_length=40)
    banio: Optional[str] = Field(default=None, max_length=40)
    clasificacion: Optional[str] = Field(default=None, max_length=40)
    ultima_fecha_compra: Optional[date] = Field(default=None)
    precio_unitario_iva_aeropuerto: Decimal = Field(..., nullable=False, ge=0)
    precio_unitario_iva_plaza: Decimal = Field(..., nullable=False, ge=0)
    costo_unitario: Decimal = Field(..., nullable=False, ge=0)
    clave_shogua: Optional[str] = Field(default=None, max_length=60)

    # # Relación con la marca (opcional, si deseas acceder a la relación)
    # marca: 'Marca' = Relationship(back_populates='productos')

    def to_dict(self):
        """Devuelve un diccionario del objeto."""
        return {
            'id_producto': self.id_producto, 
            'id_marca': self.id_marca,
            'descripcion': self.descripcion,
            'talla': self.talla,
            'departamento': self.departamento, 
            'genero': self.genero, 
            'categoria': self.categoria, 
            'subcategoria': self.subcategoria, 
            'color': self.color, 
            'disenio': self.disenio, 
            'coleccion': self.coleccion, 
            'modelo': self.modelo, 
            'material': self.material, 
            'banio': self.banio, 
            'clasificacion': self.clasificacion, 
            'ultima_fecha_compra': self.ultima_fecha_compra, 
            'precio_unitario_iva_aeropuerto': self.precio_unitario_iva_aeropuerto, 
            'precio_unitario_iva_plaza': self.precio_unitario_iva_plaza, 
            'costo_unitario': self.costo_unitario, 
            'clave_shogua': self.clave_shogua 
        }