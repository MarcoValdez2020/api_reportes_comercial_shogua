# app/core/config.py

import os
from dotenv import load_dotenv

# Carga variables de entorno desde el archivo .env
load_dotenv()

class Settings:
    # Configuración de la base de datos
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        # Configurar la URL de la base de datos
        DATABASE_URL = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}' 



# Instancia de Settings que se puede importar y usar en otras partes de la aplicación
settings = Settings()
