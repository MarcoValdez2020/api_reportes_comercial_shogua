from fastapi import APIRouter, HTTPException
from productos.producto_schemas import Producto
from productos.producto_responses import ProductoControls


from productos.producto_service import ProductoService

router = APIRouter()
producto_service = ProductoService()


@router.get("/get-all",response_model=list[Producto])
async def get_all_products():
    try:
        productos = producto_service.get_all_products()
        productos_dict = [producto.to_dict() for producto in productos]
        return productos_dict
    except Exception as e:
        print(f"Error en get_all_products: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

# @router.get("/prductos/{id_producto}",response_class=Producto)
# async def get_user_by_id(id_producto: str):
#     producto = producto_service.get_products_by_id(id_producto)
#     if not producto:
#         raise HTTPException(status_code=404, detail="Product not found")
#     return producto

@router.get("/get-controls-by-brand-name", response_model=list[ProductoControls])
async def get_all_products(
    nombre_marca:str,
    control_name:str
):
    controls = producto_service.get_products_controls_by_brand_name(nombre_marca, control_name)
    
    if len(controls)> 0:
        return controls
    elif len(controls) == 0:
        raise HTTPException(status_code=404, detail='Not found')
    else: 
        raise HTTPException(status_code=500, detail="Error interno del servidor")