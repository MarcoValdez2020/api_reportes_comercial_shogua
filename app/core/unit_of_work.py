from sqlmodel import Session
from productos.producto_repositories import ProductoRepository
from ventas.venta_repository import VentaRepository
from shared.shared_repositories import SharedRepository

from core.db_connection import database  # Importamos la instancia de Database

class UnitOfWork:
    def __init__(self):
        self.session: Session = None
        self.prducto_repository: ProductoRepository = None
        self.venta_repository: VentaRepository = None
        self.shared_repository: SharedRepository = None


    def __enter__(self):
        """Inicia la sesión de base de datos y los repositorios"""
        self.session = database.get_session()  # Obtiene la sesión sin contexto
        self.prducto_repository = ProductoRepository(self.session)
        self.venta_repository = VentaRepository(self.session)
        self.shared_repository = SharedRepository(self.session)
        
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Si todo sale bien, se hace commit; si hay error, rollback"""
        try:
            if exc_type is None:
                self.session.commit()  # Confirma la transacción si no hay excepciones
            else:
                self.session.rollback()  # Revierte si hubo una excepción
        finally:
            self.session.close()  # Cierra la sesión

    def commit(self):
        """Permite confirmar manualmente los cambios si es necesario"""
        self.session.commit()

    def rollback(self):
        """Revierte manualmente la transacción si hay un error"""
        self.session.rollback()