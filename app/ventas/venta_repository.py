from sqlmodel import Session,select
from datetime import date
from sqlalchemy import func, select, func, cast, DECIMAL, distinct
from typing import List,Tuple
from ventas.venta_schemas import Venta
from productos.producto_schemas import Producto
from shared.shared_schemas import Marca, Tienda


class VentaRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_all(self) -> list[Venta]:
        statement = select(Venta)
        result = self.session.exec(statement)
        return result.all()
    
    def get_all_by_brand_name(self, nombre_marca: str, fecha_inicio: date, fecha_fin: date) -> list[Venta]:
        statement = (
            select(Venta)
            .join(Producto, Producto.id_producto == Venta.id_producto)  # JOIN entre Venta y Producto
            .join(Marca, Marca.id_marca == Producto.id_marca)  # JOIN entre Producto y Marca
            .where(Marca.nombre == nombre_marca) # Filtro por nombre de marca
            .where(Venta.fecha >= fecha_inicio)  # Filtro por fecha de inicio
            .where(Venta.fecha <= fecha_fin)  # Filtro por fecha de fin
        )
        try:
            result = self.session.exec(statement)
        except Exception as e:
            print(f'Fallo al obtener las ventas por marca: {e}')
        return result.all()
    
    # Método de repositorio para obtener ventas agrupadas por año-mes, 
    def get_sales_grouped_by_month_and_whscode(self, nombre_marca: str, fecha_inicio: date, fecha_fin: date) -> List[Tuple[str, str, float]]:
        # Realizar la consulta utilizando SQLAlchemy (funciones como DATE_TRUNC)
        statement = (
            select(
                func.date_trunc('month', Venta.fecha).label('mes'),  # Agrupación por mes
                Venta.whscode.label('whscode'),  # Agrupación por whscode
                func.sum(Venta.venta_neta_con_iva).label('total_venta_neta_con_iva')
            )
            .join(Producto, Producto.id_producto == Venta.id_producto)  # JOIN entre Venta y Producto
            .join(Marca, Marca.id_marca == Producto.id_marca)  # JOIN entre Producto y Marca
            .where(Marca.nombre == nombre_marca)  # Filtro por nombre de marca
            .where(Venta.fecha >= fecha_inicio, Venta.fecha <= fecha_fin)  # Filtro por rango de fechas
            .group_by(func.date_trunc('month', Venta.fecha), Venta.whscode)  # Agrupamos por mes y whscode
            .order_by('mes', 'whscode')  # Ordenamos por mes y whscode
        )
        try:
            result = self.session.exec(statement)
            print(result)
        except Exception as e:
            print(f'Fallo al obtener las agrupadas: {e}')
        return result.all()        

    # Método para obtener las ventas agrupadas por año
    def get_sales_grouped_by_year_and_whscode(self, nombre_marca: str, fecha_inicio: date, fecha_fin: date) -> List[Tuple[str, str, float]]:
        # Realizar la consulta utilizando SQLAlchemy (funciones como DATE_TRUNC)
        statement = (
            select(
                func.date_trunc('year', Venta.fecha).label('anio'),  # Agrupación por mes
                Venta.whscode.label('whscode'),  # Agrupación por whscode
                func.sum(Venta.venta_neta_con_iva).label('total_venta_neta_con_iva')
            )
            .join(Producto, Producto.id_producto == Venta.id_producto)  # JOIN entre Venta y Producto
            .join(Marca, Marca.id_marca == Producto.id_marca)  # JOIN entre Producto y Marca
            .where(Marca.nombre == nombre_marca)  # Filtro por nombre de marca
            .where(Venta.fecha >= fecha_inicio, Venta.fecha <= fecha_fin)  # Filtro por rango de fechas
            .group_by(func.date_trunc('year', Venta.fecha), Venta.whscode)  # Agrupamos por mes y whscode
            .order_by('anio', 'whscode')  # Ordenamos por mes y whscode
        )
        try:
            result = self.session.exec(statement)
            print(result)
        except Exception as e:
            print(f'Fallo al obtener las agrupadas: {e}')
        return result.all()        
    
    def get_ventas_promedio_mensual_por_marca(self, nombre_marca: str,fecha_inicio:date, fecha_fin: date) -> list:  
        # Subconsulta con JOINs y filtros para obtener la suma y el promedio mensual
        statement = (
            select(
                Venta.whscode,
                func.count(distinct(func.date_trunc('month', Venta.fecha))).label("meses_con_venta"),
                func.sum(cast(Venta.cantidad, DECIMAL)).label("total_ventas"),
                (
                    func.coalesce(
                        func.sum(cast(Venta.cantidad, DECIMAL)) / func.count(distinct(func.date_trunc('month', Venta.fecha))),
                        0
                    )
                ).label("venta_promedio")
            )
            .join(Tienda, Venta.whscode == Tienda.whscode)  # JOIN entre Venta y Tienda
            .join(Marca, Tienda.id_marca == Marca.id_marca)  # JOIN entre Tienda y Marca
            .where(Marca.nombre == nombre_marca)  # Filtro por nombre de la marca
            .where(Venta.fecha >= func.date_trunc('month', fecha_inicio))  # Filtro por fecha de inicio
            .where(Venta.fecha <= fecha_fin)  # Filtro por fecha de fin
            .group_by(Venta.whscode)  # Agrupación por tienda
        )

        try:
            result = self.session.exec(statement)
            return result.all()
        except Exception as e:
            print(f'Fallo al obtener las ventas promedio mensual por marca: {e}')
            return []