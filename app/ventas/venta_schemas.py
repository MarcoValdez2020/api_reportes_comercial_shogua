from sqlmodel import SQLModel, Field, Relationship
from typing import Optional, List
from datetime import date
from decimal import Decimal

# Modelo de la tabla Ticket
class Ticket(SQLModel, table=True):
    __tablename__ = 'ticket'

    id_ticket: str = Field(primary_key=True, index=True)
    ticket: str
    fecha: date
    cantidad: int
    total: Decimal
    ventas: list['Venta'] = Relationship(back_populates='ticket_rel')

    def to_dict(self):
        """Devuelve un diccionario del objeto."""
        return {
            'id_ticket': self.id_ticket,
            'ticket': self.ticket,
            'fecha': self.fecha,
            'cantidad': self.cantidad,
            'total': self.total
        }

# Modelo de la tabla Venta
class Venta(SQLModel, table=True):
    __tablename__ = 'venta'

    id_venta: int = Field(default=None, primary_key=True)
    fecha: Optional[date] = None
    id_ticket: str = Field(foreign_key="ticket.id_ticket", nullable=False)
    id_producto: str = Field(foreign_key="producto.id_producto", nullable=False)
    whscode: str = Field(foreign_key="tienda.whscode", nullable=False)
    descuento_de_linea: Optional[int] = None
    descuento_empleado: Optional[int] = None
    cantidad: Optional[int] = None
    venta_neta_sin_iva: Decimal
    costo_total_sin_iva: Decimal
    venta_bruta_sin_iva: Decimal
    venta_neta_con_iva: Decimal
    descuento: Decimal

    ticket_rel: Ticket = Relationship(back_populates="ventas")

    def to_dict(self):
        """Devuelve un diccionario del objeto."""
        return {
            'fecha': self.fecha,
            'id_ticket': self.id_ticket,
            'id_producto': self.id_producto,
            'whscode': self.whscode,
            'descuento_de_linea': self.descuento_de_linea,
            'descuento_empleado': self.descuento_empleado,
            'cantidad': self.cantidad,
            'venta_neta_sin_iva': self.venta_neta_sin_iva,
            'costo_total_sin_iva': self.costo_total_sin_iva,
            'venta_bruta_sin_iva': self.venta_bruta_sin_iva,
            'venta_neta_con_iva': self.venta_neta_con_iva,
            'descuento': self.descuento
        }
