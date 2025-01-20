import pandas as pd
from sqlmodel import Session,select, and_
from datetime import date
from sqlalchemy import func, select, func, cast, DECIMAL, distinct, or_, case
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError

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
            .join(Tienda, Tienda.whscode == Venta.whscode)  # JOIN entre Venta y Producto
            .join(Marca, Marca.id_marca == Tienda.id_marca)  # JOIN entre Producto y Marca
            .where(Marca.nombre == nombre_marca) # Filtro por nombre de marca
            .where(Venta.fecha >= fecha_inicio, Venta.fecha <= fecha_fin)  # Filtro por rango de fechas
        )
        try:
            result = self.session.exec(statement).scalars() # Retornamos solo las instancias con scalars()
            return result.all()
        except SQLAlchemyError as e:
            print(f'Fallo al obtener las ventas por marca: {e}')
            return []
    
    # Método de repositorio para obtener ventas agrupadas por año-mes, 
    def get_sales_grouped_by_month_and_whscode(self, nombre_marca: str, fecha_inicio: date, fecha_fin: date) -> List[Tuple[str, str, float]]:
        # Realizar la consulta utilizando SQLAlchemy (funciones como DATE_TRUNC)
        statement = (
            select(
                func.date_trunc('month', Venta.fecha).label('mes'),  # Agrupación por mes
                Venta.whscode.label('whscode'),  # Agrupación por whscode
                func.sum(Venta.venta_neta_con_iva).label('total_venta_neta_con_iva')
            )
            .join(Tienda, Tienda.whscode == Venta.whscode)  # JOIN entre Venta y Producto
            .join(Marca, Marca.id_marca == Tienda.id_marca)  # JOIN entre Producto y Marca
            .where(Marca.nombre == nombre_marca)  # Filtro por nombre de marca
            .where(Venta.fecha >= fecha_inicio, Venta.fecha <= fecha_fin)  # Filtro por rango de fechas
            .group_by(func.date_trunc('month', Venta.fecha), Venta.whscode)  # Agrupamos por mes y whscode
            .order_by('mes', 'whscode')  # Ordenamos por mes y whscode
        )
        try:
            result = self.session.exec(statement)
            return result.all()
        except Exception as e:
            print(f'Fallo al obtener las agrupadas: {e}')
            return [] 

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
        print(f'{fecha_inicio} - {fecha_fin}')
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
            # Excluir ventas entre -1 y 1
            .where(or_(Venta.venta_neta_sin_iva < -1, Venta.venta_neta_sin_iva > 1))  
            .group_by(Venta.whscode)  # Agrupación por tienda
        )

        try:
            result = self.session.exec(statement)
            return result.all()
        except Exception as e:
            print(f'Fallo al obtener las ventas promedio mensual por marca: {e}')
            return []
        

    # Funcion para obtener la primera y ultima fecha de venta de una marca
    def get_first_and_last_sale_date_by_brand_name(self, nombre_marca: str):  
        # Subconsulta con JOINs y filtros para obtener la suma y el promedio mensual
        statement = (
            select(
                Marca.nombre.label('marca'),
                func.min(Venta.fecha).label('primera_fecha_venta'),
                func.max(Venta.fecha).label('ultima_fecha_venta')
            )
            .join(Producto, Venta.id_producto == Producto.id_producto)  # JOIN entre venta y producto
            .join(Marca, Producto.id_marca == Marca.id_marca)  # JOIN entre producto y marca
            .where(Marca.nombre == nombre_marca)  # Filtro por el nombre de la marca
            .group_by(Marca.nombre)  # Agrupar por el nombre de la marca
        )

        try:
            result = self.session.exec(statement)
            
            # Convertir los resultados a un DataFrame de Pandas
            df_resultados = pd.DataFrame(result)

            return df_resultados
        except Exception as e:
            print(f'Fallo al obtener la primera y ultima fecha de marca: {e}')
            return []
        
    # Funcion para obtener los años con ventas de una marca
    def get_years_with_sales_by_brand_name(self, nombre_marca: str):  
        # Subconsulta con JOINs y filtros para obtener la suma y el promedio mensual
        statement = (
            select(
                func.date_part('year', Venta.fecha).label('anio'),
                func.array_agg(distinct(func.date_part('month', Venta.fecha))).label('meses_con_venta')
            )
            .join(Tienda, Venta.whscode == Tienda.whscode)  # JOIN entre venta y tienda
            .join(Marca, Tienda.id_marca == Marca.id_marca)  # JOIN entre tienda y marca
            .where(Marca.nombre == nombre_marca)  # Filtro por el nombre de la marca
            .group_by(func.date_part('year', Venta.fecha))  # Agrupar por el año
        )

        try:
            result = self.session.exec(statement)
            return result.fetchall()
        except Exception as e:
            print(f'Fallo al obtener los años con ventas de una marca: {e}')
            return []
        

    def get_grouped_sales_by_level_by_brand(self, nombre_marca: str, fecha_inicio: str, fecha_fin: str, nivel: str):
        """
        Obtiene las ventas agrupadas por nivel (departamento, categoría, subcategoría) de una marca.

        Args:
            nombre_marca (str): El nombre de la marca por la cual filtrar los datos.
            fecha_inicio (str): La fecha de inicio para filtrar las ventas (formato 'YYYY-MM-DD').
            fecha_fin (str): La fecha de fin para filtrar las ventas (formato 'YYYY-MM-DD').
            nivel (str): Nivel de agrupación. Puede ser 'departamento', 'categoria' o 'subcategoria'.

        Returns:
            List[dict]: Lista de resultados con las ventas agrupadas por nivel, incluyendo cantidades y totales.

        Raises:
            ValueError: Si el nivel proporcionado no es válido.
        """
        # Mapear el nivel a las columnas correspondientes de Producto
        niveles_validos = {
            "departamento": [Producto.departamento],
            "categoria": [Producto.departamento, Producto.categoria],
            "subcategoria": [Producto.departamento, Producto.categoria, Producto.subcategoria],  

            "genero": [Producto.departamento, Producto.categoria, Producto.subcategoria, Producto.genero], 
            "talla": [Producto.departamento, Producto.categoria, Producto.subcategoria, Producto.talla], 
            "disenio": [Producto.departamento, Producto.categoria, Producto.subcategoria, Producto.disenio], 
            "coleccion": [Producto.departamento, Producto.categoria, Producto.subcategoria, Producto.coleccion], 
        }
        
        # Validar que el nivel sea válido
        if nivel not in niveles_validos:
            raise ValueError(f"Nivel inválido: {nivel}. Los niveles válidos son: {list(niveles_validos.keys())}")
        
        # Construir las columnas dinámicamente según el nivel
        columnas_nivel = niveles_validos[nivel]
        
        # Crear la consulta con las columnas dinámicas
        statement = (
            select(
                Tienda.whscode,
                *columnas_nivel,
                func.sum(Venta.cantidad).label('total_cantidad'),
                func.sum(Venta.venta_neta_con_iva).label('total_efectivo_con_iva')
            )
            .join(Tienda, Venta.whscode == Tienda.whscode)
            .join(Marca, Tienda.id_marca == Marca.id_marca)
            .join(Producto, Venta.id_producto == Producto.id_producto)
            .where(
                Marca.nombre == nombre_marca,
                Venta.fecha.between(fecha_inicio, fecha_fin)
            )
            .group_by(Tienda.whscode, *columnas_nivel)
            .having(
                and_(
                    func.sum(Venta.cantidad) > 0,
                    func.sum(Venta.venta_neta_con_iva) > 0
                )
            )
            .order_by(func.sum(Venta.cantidad).desc())
        )
        
        return self.session.exec(statement).all()


    def get_detail_store_report_by_brand_using_sql(self,nombre_marca: str,whscodes:list[str], fecha_inicio_mes_anio_actual: str, fecha_fin_mes_anio_actual: str, 
                                                fecha_inicio_mes_anio_anterior: str, fecha_fin_mes_anio_anterior: str, nivel: str):
        """Funcion para obtener por medio de una consulta sql el reporte detalle tienda"""
        # Mapear el nivel a las columnas correspondientes de Producto
        niveles_validos = {
            "departamento": [Producto.departamento],
            "categoria": [Producto.departamento, Producto.categoria],
            "subcategoria": [Producto.departamento, Producto.categoria, Producto.subcategoria],  

            "genero": [Producto.departamento, Producto.categoria, Producto.subcategoria, Producto.genero], 
            "talla": [Producto.departamento, Producto.categoria, Producto.subcategoria, Producto.talla], 
            "disenio": [Producto.departamento, Producto.categoria, Producto.subcategoria, Producto.disenio], 
            "coleccion": [Producto.departamento, Producto.categoria, Producto.subcategoria, Producto.coleccion], 
        }
        
        # Validar que el nivel sea válido
        if nivel not in niveles_validos:
            raise ValueError(f"Nivel inválido: {nivel}. Los niveles válidos son: {list(niveles_validos.keys())}")
        
        # Construir las columnas dinámicamente según el nivel
        columnas_nivel = niveles_validos[nivel]

        # Construccion de la consulta

        statement = (
            select(
                *columnas_nivel,  # Las categorías y subcategorías

                # Suma de cantidad de ventas del mes del año anterior
                func.sum(
                    case(
                        (Venta.fecha.between(fecha_inicio_mes_anio_anterior, fecha_fin_mes_anio_anterior), Venta.cantidad),
                        else_=0
                    )
                ).label('venta_mensual_anio_anterior_cantidad'),

                # Suma de ventas con IVA del mes del año anterior
                func.sum(
                    case(
                        (Venta.fecha.between(fecha_inicio_mes_anio_anterior, fecha_fin_mes_anio_anterior), Venta.venta_neta_con_iva),
                        else_=0
                    )
                ).label('venta_mensual_anio_anterior_iva'),

                # Suma de cantidad de ventas del mes actual
                func.sum(
                    case(
                        (Venta.fecha.between(fecha_inicio_mes_anio_actual, fecha_fin_mes_anio_actual), Venta.cantidad),
                        else_=0
                    )
                ).label('venta_mensual_anio_actual_cantidad'),

                # Suma de ventas con IVA del mes actual
                func.sum(
                    case(
                        (Venta.fecha.between(fecha_inicio_mes_anio_actual, fecha_fin_mes_anio_actual), Venta.venta_neta_con_iva),
                        else_=0
                    )
                ).label('venta_mensual_anio_actual_iva'),

                # Cálculo de la variación en porcentaje (con protección contra división por cero)
                func.coalesce(
                    (
                        func.sum(
                            case(
                                (Venta.fecha.between(fecha_inicio_mes_anio_actual, fecha_fin_mes_anio_actual), Venta.venta_neta_con_iva),
                                else_=0
                            )
                        ) / 
                        func.nullif(
                            func.sum(
                                case(
                                    (Venta.fecha.between(fecha_inicio_mes_anio_anterior, fecha_fin_mes_anio_anterior), Venta.venta_neta_con_iva),
                                    else_=0
                                )
                            ), 0
                        )
                    ) - 1
                ).label('variacion_mes_porcentaje'),

                # Cálculo de la variación en efectivo
                (
                    func.sum(
                        case(
                            (Venta.fecha.between(fecha_inicio_mes_anio_actual, fecha_fin_mes_anio_actual), Venta.venta_neta_con_iva),
                            else_=0
                        )
                    ) - func.sum(
                        case(
                            (Venta.fecha.between(fecha_inicio_mes_anio_anterior, fecha_fin_mes_anio_anterior), Venta.venta_neta_con_iva),
                            else_=0
                        )
                    )
                ).label('variacion_mes_efectivo')
            )
            .select_from(Venta)
            .join(Tienda, Tienda.whscode == Venta.whscode)
            .join(Marca, Tienda.id_marca == Marca.id_marca)
            .join(Producto, Venta.id_producto == Producto.id_producto)
            .where(
                Tienda.whscode.in_(whscodes),
                Marca.nombre == nombre_marca
            )
            .group_by(*columnas_nivel)
            .having(
                and_(
                    func.sum(Venta.cantidad) > 0,
                    func.sum(Venta.venta_neta_con_iva) > 0
                )
            )
            .order_by(func.sum(Venta.cantidad).desc())
        )

        return self.session.exec(statement).all()
    
