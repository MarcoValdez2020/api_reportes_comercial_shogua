from fastapi import FastAPI, HTTPException
from sqlmodel import text
from core.db_connection import database

app = FastAPI()

@app.get("/test-connection")
def test_connection():
    try:
        # Abrir una sesión y ejecutar una consulta simple
        with database.get_session() as session:
            # Ejecutar una consulta simple para verificar la conexión
            result = session.exec(text("SELECT NOW()")).first()
            return {"status": "success", "current_time": result[0]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Connection test failed: {e}")

