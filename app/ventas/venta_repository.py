import pandas as pd
from sqlmodel import Session,select, and_
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import func, literal, select, func, cast, DECIMAL, distinct, or_, case
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
        
    #* Funciones para las ventas a detalle tienda

    # Función para obtener el reporte de detalle tienda
    def get_hierarchical_sales_report(
        self,
        nombre_marca: str,
        whscodes: list[str],
        fecha_inicio_mes_anio_actual: str,
        fecha_fin_mes_anio_actual: str,
        fecha_inicio_mes_anio_anterior: str,
        fecha_fin_mes_anio_anterior: str,
        tipo_inventario: str,
        tallas: list[str] = None,
        generos: list[str] = None,
        disenios: list[str] = None,
        colecciones: list[str] = None,
    ):
        """
        Genera y ejecuta un reporte jerárquico de ventas (departamento -> categoría -> subcategoría)
        considerando las ventas del año anterior y el año actual, filtrando por tallas, géneros,
        diseños y colecciones, utilizando parámetros para prevenir SQL Injection.
        """

        # Verificar el tipo de inventario
        if tipo_inventario == 'actual':
            # Se hará el join con la tabla de inventario_tienda
            tabla_inventario = 'inventario_tienda'
            filtro_fecha = ''

        elif tipo_inventario == 'cierre-mes':
            # Se hará el join con la tabla histiorial_inventario_tienda
            tabla_inventario = 'historial_inventario_tienda'
            
            # Se calcula la fecha de cierre de mes segun el mes actual
            # Convertir la cadena a un objeto datetime
            fecha = datetime.strptime(fecha_inicio_mes_anio_actual, "%Y-%m-%d")

            # Sumar un mes (automáticamente maneja diciembre y cambia de año si es necesario)
            fecha_mas_un_mes = fecha + relativedelta(months=1)

            # Convertir de nuevo a cadena
            fecha_cierre_mes = fecha_mas_un_mes.strftime("%Y-%m-%d")
            
            filtro_fecha = f"AND {tabla_inventario}.fecha = '{fecha_cierre_mes}'"

        # Generar las condiciones dinámicas
        condiciones = [
            "tienda.whscode = ANY (:whscodes)",
            "marca.nombre = :nombre_marca",
        ]

        if tallas:
            condiciones.append("producto.talla = ANY (:tallas)")
        if generos:
            condiciones.append("producto.genero = ANY (:generos)")
        if disenios:
            condiciones.append("producto.disenio = ANY (:disenios)")
        if colecciones:
            condiciones.append("producto.coleccion = ANY (:colecciones)")

        # Unir las condiciones en una cláusula WHERE
        where_clause = " AND ".join(condiciones)

        # Condicion de query con marcas
        if nombre_marca == 'AY GÜEY':
            # Si la marca es AG el query solo trabajara con el nivel departamento -> categoria
            query = f"""
            WITH 
                variables AS (
                    SELECT
                        '{fecha_inicio_mes_anio_anterior}'::date AS fecha_inicio_anio_anterior,
                        '{fecha_fin_mes_anio_anterior}'::date AS fecha_fin_anio_anterior,
                        '{fecha_inicio_mes_anio_actual}'::date AS fecha_inicio_anio_actual,
                        '{fecha_fin_mes_anio_actual}'::date AS fecha_fin_anio_actual
                ),
                VentasPorProducto AS (
                    SELECT 
                        producto.departamento,
                        producto.categoria,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_anterior AND variables.fecha_fin_anio_anterior 
                                THEN venta.cantidad ELSE 0 END) AS cantidad_anio_anterior,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_anterior AND variables.fecha_fin_anio_anterior 
                                THEN venta.venta_neta_con_iva ELSE 0 END) AS iva_anio_anterior,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_actual AND variables.fecha_fin_anio_actual 
                                THEN venta.cantidad ELSE 0 END) AS cantidad_anio_actual,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_actual AND variables.fecha_fin_anio_actual 
                                THEN venta.venta_neta_con_iva ELSE 0 END) AS iva_anio_actual
                    FROM venta
                    JOIN tienda ON tienda.whscode = venta.whscode
                    JOIN marca ON tienda.id_marca = marca.id_marca
                    JOIN producto ON venta.id_producto = producto.id_producto
                    CROSS JOIN variables
                    WHERE {where_clause}
                    GROUP BY producto.departamento, producto.categoria
                ),
                InventarioPorProducto AS (
                    SELECT
                        producto.departamento,
                        producto.categoria,
                        producto.subcategoria,
                        SUM(COALESCE({tabla_inventario}.existencia, 0)) AS existencia
                    FROM {tabla_inventario}
                    JOIN producto ON {tabla_inventario}.id_producto = producto.id_producto
                    JOIN tienda ON tienda.whscode = {tabla_inventario}.whscode
                    JOIN marca ON tienda.id_marca = marca.id_marca
                    WHERE {where_clause} {filtro_fecha}
                    GROUP BY producto.departamento, producto.categoria, producto.subcategoria
                ),
                Variaciones AS (
                    SELECT 
                        departamento,
                        categoria,
                        cantidad_anio_anterior,
                        iva_anio_anterior,
                        cantidad_anio_actual,
                        iva_anio_actual,
                        COALESCE((cantidad_anio_actual / NULLIF(cantidad_anio_anterior, 0)) - 1, 0) * 100 AS variacion_mes_porcentaje_cantidad,
                        COALESCE(cantidad_anio_actual - cantidad_anio_anterior, 0) AS variacion_mes_cantidad,
                        COALESCE((iva_anio_actual / NULLIF(iva_anio_anterior, 0)) - 1, 0) * 100 AS variacion_porcentaje,
                        COALESCE(iva_anio_actual - iva_anio_anterior, 0) AS variacion_efectivo
                    FROM VentasPorProducto
                ),
                TotalesPorNivel AS (
                    SELECT 
                        'DEPARTAMENTO' AS nivel,
                        departamento AS key,
                        departamento AS nombre,
                        SUM(cantidad_anio_anterior) AS cantidad_anio_anterior,
                        SUM(iva_anio_anterior) AS iva_anio_anterior,
                        SUM(cantidad_anio_actual) AS cantidad_anio_actual,
                        SUM(iva_anio_actual) AS iva_anio_actual,
                        COALESCE((SUM(cantidad_anio_actual) / NULLIF(SUM(cantidad_anio_anterior), 0)) - 1, 0) * 100 AS variacion_mes_porcentaje_cantidad,
                        COALESCE(SUM(cantidad_anio_actual) - SUM(cantidad_anio_anterior), 0) AS variacion_mes_cantidad,
                        COALESCE((SUM(iva_anio_actual) / NULLIF(SUM(iva_anio_anterior), 0)) - 1, 0) * 100 AS variacion_porcentaje,
                        COALESCE(SUM(iva_anio_actual) - SUM(iva_anio_anterior), 0) AS variacion_efectivo,
                        SUM(existencia) AS existencia
                    FROM Variaciones
                    LEFT JOIN InventarioPorProducto USING (departamento)
                    GROUP BY departamento

                    UNION ALL

                    SELECT 
                        'CATEGORIA' AS nivel,
                        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') AS key,
                        COALESCE(categoria, 'SIN CATEGORIA') AS nombre,
                        SUM(cantidad_anio_anterior),
                        SUM(iva_anio_anterior),
                        SUM(cantidad_anio_actual),
                        SUM(iva_anio_actual),
                        COALESCE((SUM(cantidad_anio_actual) / NULLIF(SUM(cantidad_anio_anterior), 0)) - 1, 0) * 100,
                        COALESCE(SUM(cantidad_anio_actual) - SUM(cantidad_anio_anterior), 0),
                        COALESCE((SUM(iva_anio_actual) / NULLIF(SUM(iva_anio_anterior), 0)) - 1, 0) * 100,
                        COALESCE(SUM(iva_anio_actual) - SUM(iva_anio_anterior), 0),
                        SUM(existencia)
                    FROM Variaciones
                    LEFT JOIN InventarioPorProducto USING (departamento, categoria)
                    GROUP BY departamento, categoria
                )
            SELECT *
            FROM TotalesPorNivel
            WHERE cantidad_anio_actual > 0
            ORDER BY nivel, key;
            """


        elif nombre_marca == 'MUMUSO':
            # Si la marca es mumuso trabajara con los 3 niveles

            # Consulta SQL con placeholders
            query = f"""
            WITH 
                variables AS (
                    SELECT
                        '{fecha_inicio_mes_anio_anterior}'::date AS fecha_inicio_anio_anterior,
                        '{fecha_fin_mes_anio_anterior}'::date AS fecha_fin_anio_anterior,
                        '{fecha_inicio_mes_anio_actual}'::date AS fecha_inicio_anio_actual,
                        '{fecha_fin_mes_anio_actual}'::date AS fecha_fin_anio_actual
                ),
                VentasPorProducto AS (
                    SELECT 
                        producto.departamento,
                        producto.categoria,
                        producto.subcategoria,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_anterior AND variables.fecha_fin_anio_anterior 
                                THEN venta.cantidad ELSE 0 END) AS cantidad_anio_anterior,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_anterior AND variables.fecha_fin_anio_anterior 
                                THEN venta.venta_neta_con_iva ELSE 0 END) AS iva_anio_anterior,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_actual AND variables.fecha_fin_anio_actual 
                                THEN venta.cantidad ELSE 0 END) AS cantidad_anio_actual,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_actual AND variables.fecha_fin_anio_actual 
                                THEN venta.venta_neta_con_iva ELSE 0 END) AS iva_anio_actual
                    FROM venta
                    JOIN tienda ON tienda.whscode = venta.whscode
                    JOIN marca ON tienda.id_marca = marca.id_marca
                    JOIN producto ON venta.id_producto = producto.id_producto
                    CROSS JOIN variables
                    WHERE {where_clause}
                    GROUP BY producto.departamento, producto.categoria, producto.subcategoria
                ),
                InventarioPorProducto AS (
                    SELECT
                        producto.departamento,
                        producto.categoria,
                        producto.subcategoria,
                        SUM(inventario_tienda.existencia) AS existencia
                    FROM inventario_tienda
                    JOIN producto ON inventario_tienda.id_producto = producto.id_producto
                    JOIN tienda ON tienda.whscode = inventario_tienda.whscode
                    JOIN marca ON tienda.id_marca = marca.id_marca
                    WHERE {where_clause}
                    GROUP BY producto.departamento, producto.categoria, producto.subcategoria
                ),
                Variaciones AS (
                    SELECT 
                        departamento,
                        categoria,
                        subcategoria,
                        cantidad_anio_anterior,
                        iva_anio_anterior,
                        cantidad_anio_actual,
                        iva_anio_actual,
                        COALESCE((cantidad_anio_actual / NULLIF(cantidad_anio_anterior, 0)) - 1, 0) * 100 AS variacion_mes_porcentaje_cantidad,
                        COALESCE(cantidad_anio_actual - cantidad_anio_anterior, 0) AS variacion_mes_cantidad,
                        COALESCE((iva_anio_actual / NULLIF(iva_anio_anterior, 0)) - 1, 0) * 100 AS variacion_porcentaje,
                        COALESCE(iva_anio_actual - iva_anio_anterior, 0) AS variacion_efectivo
                    FROM VentasPorProducto
                ),
                TotalesPorNivel AS (
                    SELECT 
                        'DEPARTAMENTO' AS nivel,
                        departamento AS key,
                        departamento AS nombre,
                        SUM(cantidad_anio_anterior) AS cantidad_anio_anterior,
                        SUM(iva_anio_anterior) AS iva_anio_anterior,
                        SUM(cantidad_anio_actual) AS cantidad_anio_actual,
                        SUM(iva_anio_actual) AS iva_anio_actual,
                        COALESCE((SUM(cantidad_anio_actual) / NULLIF(SUM(cantidad_anio_anterior), 0)) - 1, 0) * 100 AS variacion_mes_porcentaje_cantidad,
                        COALESCE(SUM(cantidad_anio_actual) - SUM(cantidad_anio_anterior), 0) AS variacion_mes_cantidad,
                        COALESCE((SUM(iva_anio_actual) / NULLIF(SUM(iva_anio_anterior), 0)) - 1, 0) * 100 AS variacion_porcentaje,
                        COALESCE(SUM(iva_anio_actual) - SUM(iva_anio_anterior), 0) AS variacion_efectivo,
                        SUM(existencia) AS existencia
                    FROM Variaciones
                    LEFT JOIN InventarioPorProducto USING (departamento)
                    GROUP BY departamento

                    UNION ALL

                    SELECT 
                        'CATEGORIA' AS nivel,
                        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') AS key,
                        COALESCE(categoria, 'SIN CATEGORIA') AS nombre,
                        SUM(cantidad_anio_anterior),
                        SUM(iva_anio_anterior),
                        SUM(cantidad_anio_actual),
                        SUM(iva_anio_actual),
                        COALESCE((SUM(cantidad_anio_actual) / NULLIF(SUM(cantidad_anio_anterior), 0)) - 1, 0) * 100,
                        COALESCE(SUM(cantidad_anio_actual) - SUM(cantidad_anio_anterior), 0),
                        COALESCE((SUM(iva_anio_actual) / NULLIF(SUM(iva_anio_anterior), 0)) - 1, 0) * 100,
                        COALESCE(SUM(iva_anio_actual) - SUM(iva_anio_anterior), 0),
                        SUM(existencia)
                    FROM Variaciones
                    LEFT JOIN InventarioPorProducto USING (departamento, categoria)
                    GROUP BY departamento, categoria

                    UNION ALL

                    SELECT 
                        'SUBCATEGORIA' AS nivel,
                        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') || '-' || COALESCE(subcategoria, 'SIN SUBCATEGORIA') AS key,
                        COALESCE(subcategoria, 'SIN SUBCATEGORIA') AS nombre,
                        cantidad_anio_anterior,
                        iva_anio_anterior,
                        cantidad_anio_actual,
                        iva_anio_actual,
                        variacion_mes_porcentaje_cantidad,
                        variacion_mes_cantidad,
                        variacion_porcentaje,
                        variacion_efectivo,
                        existencia
                    FROM Variaciones
                    LEFT JOIN InventarioPorProducto USING (departamento, categoria, subcategoria)
                )
            SELECT *
            FROM TotalesPorNivel
            WHERE NOT (cantidad_anio_actual <= 0 AND existencia <= 0)
            ORDER BY nivel, key;
            """
        
        
        else:
            # Si no es AG ni mumuso entonces se trabaja a nivel categoria y subcategoria
            query = f"""
            WITH 
                variables AS (
                    SELECT
                        '{fecha_inicio_mes_anio_anterior}'::date AS fecha_inicio_anio_anterior,
                        '{fecha_fin_mes_anio_anterior}'::date AS fecha_fin_anio_anterior,
                        '{fecha_inicio_mes_anio_actual}'::date AS fecha_inicio_anio_actual,
                        '{fecha_fin_mes_anio_actual}'::date AS fecha_fin_anio_actual
                ),
                VentasPorProducto AS (
                    SELECT 
                        producto.categoria,
                        producto.subcategoria,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_anterior AND variables.fecha_fin_anio_anterior 
                                THEN venta.cantidad ELSE 0 END) AS cantidad_anio_anterior,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_anterior AND variables.fecha_fin_anio_anterior 
                                THEN venta.venta_neta_con_iva ELSE 0 END) AS iva_anio_anterior,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_actual AND variables.fecha_fin_anio_actual 
                                THEN venta.cantidad ELSE 0 END) AS cantidad_anio_actual,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_actual AND variables.fecha_fin_anio_actual 
                                THEN venta.venta_neta_con_iva ELSE 0 END) AS iva_anio_actual
                    FROM venta
                    JOIN tienda ON tienda.whscode = venta.whscode
                    JOIN marca ON tienda.id_marca = marca.id_marca
                    JOIN producto ON venta.id_producto = producto.id_producto
                    CROSS JOIN variables
                    WHERE {where_clause}
                    GROUP BY producto.categoria, producto.subcategoria
                ),
                InventarioPorProducto AS (
                    SELECT
                        producto.categoria,
                        producto.subcategoria,
                        SUM(inventario_tienda.existencia) AS existencia
                    FROM inventario_tienda
                    JOIN producto ON inventario_tienda.id_producto = producto.id_producto
                    JOIN tienda ON tienda.whscode = inventario_tienda.whscode
                    JOIN marca ON tienda.id_marca = marca.id_marca
                    WHERE {where_clause}
                    GROUP BY producto.categoria, producto.subcategoria
                ),
                Variaciones AS (
                    SELECT 
                        categoria,
                        subcategoria,
                        cantidad_anio_anterior,
                        iva_anio_anterior,
                        cantidad_anio_actual,
                        iva_anio_actual,
                        COALESCE((cantidad_anio_actual / NULLIF(cantidad_anio_anterior, 0)) - 1, 0) * 100 AS variacion_mes_porcentaje_cantidad,
                        COALESCE(cantidad_anio_actual - cantidad_anio_anterior, 0) AS variacion_mes_cantidad,
                        COALESCE((iva_anio_actual / NULLIF(iva_anio_anterior, 0)) - 1, 0) * 100 AS variacion_porcentaje,
                        COALESCE(iva_anio_actual - iva_anio_anterior, 0) AS variacion_efectivo
                    FROM VentasPorProducto
                ),
                TotalesPorNivel AS (
                    SELECT 
                        'CATEGORIA' AS nivel,
                        categoria AS key,
                        categoria AS nombre,
                        SUM(cantidad_anio_anterior) AS cantidad_anio_anterior,
                        SUM(iva_anio_anterior) AS iva_anio_anterior,
                        SUM(cantidad_anio_actual) AS cantidad_anio_actual,
                        SUM(iva_anio_actual) AS iva_anio_actual,
                        COALESCE((SUM(cantidad_anio_actual) / NULLIF(SUM(cantidad_anio_anterior), 0)) - 1, 0) * 100 AS variacion_mes_porcentaje_cantidad,
                        COALESCE(SUM(cantidad_anio_actual) - SUM(cantidad_anio_anterior), 0) AS variacion_mes_cantidad,
                        COALESCE((SUM(iva_anio_actual) / NULLIF(SUM(iva_anio_anterior), 0)) - 1, 0) * 100 AS variacion_porcentaje,
                        COALESCE(SUM(iva_anio_actual) - SUM(iva_anio_anterior), 0) AS variacion_efectivo,
                        SUM(existencia) AS existencia
                    FROM Variaciones
                    LEFT JOIN InventarioPorProducto USING (categoria)
                    GROUP BY categoria

                    UNION ALL

                    SELECT 
                        'SUBCATEGORIA' AS nivel,
                        categoria || '-' || COALESCE(subcategoria, 'SIN CATEGORIA') AS key,
                        COALESCE(subcategoria, 'SIN CATEGORIA') AS nombre,
                        SUM(cantidad_anio_anterior),
                        SUM(iva_anio_anterior),
                        SUM(cantidad_anio_actual),
                        SUM(iva_anio_actual),
                        COALESCE((SUM(cantidad_anio_actual) / NULLIF(SUM(cantidad_anio_anterior), 0)) - 1, 0) * 100,
                        COALESCE(SUM(cantidad_anio_actual) - SUM(cantidad_anio_anterior), 0),
                        COALESCE((SUM(iva_anio_actual) / NULLIF(SUM(iva_anio_anterior), 0)) - 1, 0) * 100,
                        COALESCE(SUM(iva_anio_actual) - SUM(iva_anio_anterior), 0),
                        SUM(existencia)
                    FROM Variaciones
                    LEFT JOIN InventarioPorProducto USING (categoria, subcategoria)
                    GROUP BY categoria, subcategoria

                )
            SELECT *
            FROM TotalesPorNivel
            WHERE cantidad_anio_actual > 0
            ORDER BY nivel, key;
            """
        # Parámetros seguros para la consulta
        params = {
            "nombre_marca": nombre_marca,
            "whscodes": whscodes,
            "fecha_inicio_mes_anio_actual": fecha_inicio_mes_anio_actual,
            "fecha_fin_mes_anio_actual": fecha_fin_mes_anio_actual,
            "fecha_inicio_mes_anio_anterior": fecha_inicio_mes_anio_anterior,
            "fecha_fin_mes_anio_anterior": fecha_fin_mes_anio_anterior,
            "tallas": tallas,
            "generos": generos,
            "disenios": disenios,
            "colecciones": colecciones,
        }

        # Convertir la consulta a un objeto SQL seguro
        query = text(query)
        

        # Ejecutar la consulta en la base de datos
        result = self.session.execute(query, params).fetchall()

        return result
    

    # Funciones para filtrar los campos de detalle tienda
    def filtrar_campos_detalle_tienda(
            self,
            whscodes: list[str], 
            fecha_inicio_mes_anio_actual: str,
            fecha_fin_mes_anio_actual: str,
            fecha_inicio_mes_anio_anterior: str,
            fecha_fin_mes_anio_anterior: str,
            tallas:list[str] = None, 
            generos:list[str] = None, 
            disenios:list[str] = None, 
            colecciones:list[str] = None): 
        """Función para ir filtrando los campos del front-end segun el o los parametros seleccionados (genero, coleccion, talla y diseño)"""
        
        # Base de la consulta
        query = (
            select(
                Producto.talla,
                Producto.genero,
                Producto.disenio,
                Producto.coleccion
            )
            .select_from(Venta)
            .join(Tienda, Tienda.whscode == Venta.whscode)
            .join(Producto, Venta.id_producto == Producto.id_producto)
            .where(
                Tienda.whscode.in_(whscodes),
                or_(
                    Venta.fecha.between(fecha_inicio_mes_anio_actual, fecha_fin_mes_anio_actual),
                    Venta.fecha.between(fecha_inicio_mes_anio_anterior, fecha_fin_mes_anio_anterior)
                )
            )
        )

        # Filtros dinámicos multiselect
        if tallas and len(tallas) > 0:
            query = query.where(Producto.talla.in_(tallas))
        if disenios and len(disenios) > 0:
            query = query.where(Producto.disenio.in_(disenios))
        if generos and len(generos) > 0:
            query = query.where(Producto.genero.in_(generos))
        if colecciones and len(colecciones) > 0:
            query = query.where(Producto.coleccion.in_(colecciones))

        # Asegurar combinaciones únicas
        query = query.distinct().order_by(
            Producto.talla,
            Producto.genero,
            Producto.disenio,
            Producto.coleccion
        )

        # Ejecutar la consulta
        return self.session.exec(query).all()