from sqlmodel import SQLModel, Session, create_engine
from contextlib import contextmanager
from core.config import settings  # Importar la configuración

class Database:
    def __init__(self):
        # Utilizar la URL de la base de datos desde settings
        self.engine = create_engine(settings.DATABASE_URL, echo=True)

        # # Crear las tablas en la base de datos
        # SQLModel.metadata.create_all(self.engine)
        
    # Proveer una sesión de base de datos usando un contexto
    @contextmanager
    def get_session(self):
        with Session(self.engine) as session:
            yield session

# Instancia global de la base de datos
database = Database()