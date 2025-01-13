from fastapi import APIRouter, HTTPException

from shared.shared_service import SharedService
from shared.shared_schemas import Marca, Tienda


router = APIRouter()
shared_service = SharedService()


# Ruta expuesta de las marcas
@router.get("/get-all-brands", response_model=list[Marca])
async def get_all_brands():
    try:
        brands = shared_service.get_all_brands()
        return brands
    except Exception as e:
        print(f"Error en get_all_brands: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")


# Ruta expuesta de las tiendas por nombre de marca
@router.get("/get-all-stores-by-brand-name", response_model=list[Tienda])
async def get_all_stores_by_brand_name(
    nombre_marca:str, 
):
    try:
        stores = shared_service.get_all_stores_by_brand_name(nombre_marca)
        return stores
    except Exception as e:
        print(f"Error en get_all_brands: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
    