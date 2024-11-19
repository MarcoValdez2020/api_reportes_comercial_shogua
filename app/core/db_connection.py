from sqlmodel import SQLModel, Session, create_engine
from contextlib import contextmanager
from core.config import settings  # Importar la configuración

class Database:
    def __init__(self):
        # Utilizar la URL de la base de datos desde settings
        self.engine = create_engine(settings.DATABASE_URL, echo=False)

        # # Crear las tablas en la base de datos
        # SQLModel.metadata.create_all(self.engine)
        
    def get_session(self) -> Session:
        # Devuelve una sesión sin un generador; UnitOfWork gestionará la apertura y cierre
        return Session(self.engine, expire_on_commit=False)

# Instancia global de la base de datos
database = Database()