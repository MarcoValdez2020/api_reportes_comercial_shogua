from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import text

from core.db_connection import database
from productos.producto_routes import router as product_router  # Importa el router del submódulo productos
from ventas.venta_routes import router as ventas_router  # Importa el router del submódulo ventas
from reports.report_routes import router as reports_router # Importar el router del submódulo reportes
from shared.shared_routes import router as shared_router # Importar el router del submódulo reportes


app = FastAPI()

origins = [
    "http://localhost:4200",
    "http://localhost",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"message": "Hello Fast API"}


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


# Incluir las rutas de productos en la aplicación principal
app.include_router(product_router, prefix="/productos", tags=["Productos"])
app.include_router(ventas_router, prefix="/ventas", tags=["Ventas"])
app.include_router(reports_router, prefix="/reportes", tags=["Reportes"])
app.include_router(shared_router, prefix="/shared", tags=["Shared"])
