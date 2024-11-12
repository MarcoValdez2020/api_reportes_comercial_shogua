from core.unit_of_work import UnitOfWork
from typing import List, Optional
from fastapi import HTTPException

from shared.shared_schemas import Marca, Tienda, Almacen

class SharedService:
    def __init__(self):
        # En caso de usar inyeccion de dependencias se pasa aqui :)
        pass

    def get_all_brands(self) -> List[Marca]:
        """Obtener todas las marcas de la base de datos."""
        with UnitOfWork() as uow:
            # Obtener todos registros
            try:
                return uow.shared_repository.get_all_brands()
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener marcas: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener marcas")
    
    def get_all_stores_by_brand_name(self,nombre_marca:str) -> List[Tienda]:
            """Obtener todas las marcas de la base de datos."""
            with UnitOfWork() as uow:
                # Obtener todos registros
                try:
                    return uow.shared_repository.get_all_stores_by_brand_name(nombre_marca)
                except Exception as e:
                        # Log de la excepción para saber el error
                        print(f"Error al obtener tiendas: {e}")
                        raise HTTPException(status_code=500, detail="Error al obtener marcas")
    
