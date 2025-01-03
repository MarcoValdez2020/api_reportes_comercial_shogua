from fastapi import APIRouter, HTTPException
from datetime import date

from inventarios.inventario_service import InventarioService

router = APIRouter()
inventario_service = InventarioService()

# Ruta expuesta para el obtener los años con inventarios de cierre de mes disponibles
@router.get("/get-end-month-history-inventory-list")
async def get_end_month_history_inventory_list(
    nombre_marca:str
):
    try:
        lista_inventarios = inventario_service.get_end_month_history_inventory_list(nombre_marca)
        return lista_inventarios
    except Exception as e:
        print(f"Error en get_inventory_end_month_years_list: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")