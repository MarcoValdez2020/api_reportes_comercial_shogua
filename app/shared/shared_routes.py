from fastapi import APIRouter, HTTPException

from shared.shared_service import SharedService
from shared.shared_schemas import Marca


router = APIRouter()
shared_service = SharedService()


# Ruta expuesta para el reporte de cierre de mes de AG y mumuso
@router.get("/get-all-brands", response_model=list[Marca])
async def get_all_brands():
    try:
        cierre_mes = shared_service.get_all_brands()
        return cierre_mes
    except Exception as e:
        print(f"Error en get_all_brands: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    