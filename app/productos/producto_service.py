from core.unit_of_work import UnitOfWork
from productos.producto_schemas import Producto
from typing import List, Optional
from fastapi import APIRouter, HTTPException

class ProductoService:
    def __init__(self):
        # En caso de usar inyeccion de dependencias se pasa aqui :)
        pass

    def get_all_products(self) -> List[Producto]:
        """Obtener todos los productos de la base de datos."""
        with UnitOfWork() as uow:
            # Obtener todos los productos
            try:
                return uow.prducto_repository.get_all()
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener productos: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener productos")
            
    def get_products_by_id(self, id_producto: str) -> Optional[Producto]:
        """Obtener un producto por su ID."""
        with UnitOfWork() as uow:
            # Obtener el registro por ID
            return uow.prducto_repository.get_by_id(id_producto)
        
    def get_products_controls_by_brand_name(self, nombre_marca:str, control_name:str):
        """Obtener el valor unico de tallas de producto por marca"""
        with UnitOfWork() as uow:
            try:
                # Obtener tallas por nombre de marca
                data = uow.prducto_repository.getAllProductControlsByBrandName(nombre_marca, control_name)
                response = [{'control_name':control_name, 'value':value} for value in data]
                return response
            except Exception as e:
                # Log de la excepción para saber el error
                print(f"Error al obtener productos: {e}")
                raise HTTPException(status_code=500, detail="Error al obtener el producto")


