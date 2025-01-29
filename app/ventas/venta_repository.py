import pandas as pd
from sqlmodel import Session,select, and_
from datetime import date, datetime, timedelta
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
        fecha_inicio_periodo_ventas_prom: str, 
        fecha_fin_periodo_ventas_prom: str,
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
        # Calculamos la fecha de cierre de mes del año anterior sumando un dia a la fecha de fin de mes del año anterior
        fecha_fin_mes_anio_anterior_aux = datetime.strptime(fecha_fin_mes_anio_anterior, "%Y-%m-%d")
        fecha_cierre_anio_anterior = (fecha_fin_mes_anio_anterior_aux + timedelta(days=1)).strftime("%Y-%m-%d")

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
                        '{fecha_fin_mes_anio_actual}'::date AS fecha_fin_anio_actual,
                        '{fecha_cierre_anio_anterior}'::date AS fecha_cierre_anio_anterior,
                        '{fecha_inicio_periodo_ventas_prom}'::date AS fecha_inicio_periodo_ventas_prom,
                        '{fecha_fin_periodo_ventas_prom}'::date AS fecha_fin_periodo_ventas_prom
                ),
                VentasPorProducto AS (
                    SELECT 
                        producto.departamento,
                        producto.categoria,
                        producto.subcategoria,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_anterior AND variables.fecha_fin_anio_anterior 
                            THEN venta.venta_neta_con_iva ELSE 0 END) AS iva_anio_anterior,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_actual AND variables.fecha_fin_anio_actual 
                            THEN venta.cantidad ELSE 0 END) AS cantidad_anio_actual,
                        SUM(CASE WHEN venta.fecha BETWEEN variables.fecha_inicio_anio_anterior AND variables.fecha_fin_anio_anterior 
                            THEN venta.cantidad ELSE 0 END) AS cantidad_anio_anterior,
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
                VariacionesVentas AS (
                    SELECT 
                        departamento,
                        categoria,
                        subcategoria,
                        iva_anio_anterior,
                        iva_anio_actual,
                        COALESCE(iva_anio_actual - iva_anio_anterior, 0) AS variacion_efectivo,
                        COALESCE((iva_anio_actual / NULLIF(iva_anio_anterior, 0)) - 1, 0) AS variacion_porcentaje,
                        cantidad_anio_anterior,
                        cantidad_anio_actual,
                        COALESCE(cantidad_anio_actual - cantidad_anio_anterior, 0) AS variacion_mes_cantidad,
                        ROUND(COALESCE((CAST(cantidad_anio_actual AS numeric) / NULLIF(CAST(cantidad_anio_anterior AS numeric), 0)) - 1, 0), 2) AS variacion_mes_porcentaje_cantidad
                    FROM VentasPorProducto
                ),
                TotalesVariacionesPorNivel AS (
                    SELECT 
                        'DEPARTAMENTO' AS nivel,
                        departamento AS key,
                        departamento AS nombre,
                        SUM(iva_anio_anterior) AS iva_anio_anterior,
                        SUM(iva_anio_actual) AS iva_anio_actual,
                        COALESCE(SUM(iva_anio_actual) - SUM(iva_anio_anterior), 0) AS variacion_efectivo,
                        COALESCE((SUM(iva_anio_actual) / NULLIF(SUM(iva_anio_anterior), 0)) - 1, 0) AS variacion_porcentaje,
                        SUM(cantidad_anio_anterior) AS cantidad_anio_anterior,
                        SUM(cantidad_anio_actual) AS cantidad_anio_actual,
                        COALESCE(SUM(cantidad_anio_actual) - SUM(cantidad_anio_anterior), 0) AS variacion_mes_cantidad,
                        COALESCE((SUM(cantidad_anio_actual) / NULLIF(SUM(cantidad_anio_anterior), 0)) - 1, 0) AS variacion_mes_porcentaje_cantidad
                    FROM VariacionesVentas
                    GROUP BY departamento

                    UNION ALL

                    SELECT 
                        'CATEGORIA' AS nivel,
                        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') AS key,
                        COALESCE(categoria, 'SIN CATEGORIA') AS nombre,
                        SUM(iva_anio_anterior),
                        SUM(iva_anio_actual),
                        COALESCE(SUM(iva_anio_actual) - SUM(iva_anio_anterior), 0),
                        COALESCE((SUM(iva_anio_actual) / NULLIF(SUM(iva_anio_anterior), 0)) - 1, 0),
                        SUM(cantidad_anio_anterior),
                        SUM(cantidad_anio_actual),
                        COALESCE(SUM(cantidad_anio_actual) - SUM(cantidad_anio_anterior), 0),
                        COALESCE((SUM(cantidad_anio_actual) / NULLIF(SUM(cantidad_anio_anterior), 0)) - 1, 0)
                    FROM VariacionesVentas
                    GROUP BY departamento, categoria

                    UNION ALL

                    SELECT 
                        'SUBCATEGORIA' AS nivel,
                        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') || '-' || COALESCE(subcategoria, 'SIN SUBCATEGORIA') AS key,
                        COALESCE(subcategoria, 'SIN SUBCATEGORIA') AS nombre,
                        iva_anio_anterior,
                        iva_anio_actual,
                        variacion_efectivo,
                        variacion_porcentaje,
                        cantidad_anio_anterior,
                        cantidad_anio_actual,
                        variacion_mes_cantidad,
                        variacion_mes_porcentaje_cantidad
                    FROM VariacionesVentas
                ),
                InventarioTiendaPorNivel AS (
                    SELECT 
                        'DEPARTAMENTO' AS nivel,
                        departamento AS key,
                        SUM(existencia) AS existencia_tienda
                    FROM {tabla_inventario}
                    JOIN producto ON producto.id_producto = {tabla_inventario}.id_producto
                    JOIN tienda ON tienda.whscode = {tabla_inventario}.whscode
                    JOIN marca ON marca.id_marca = tienda.id_marca
                    CROSS JOIN variables
                    WHERE {where_clause} {filtro_fecha}
                    GROUP BY producto.departamento

                    UNION ALL

                    SELECT 
                        'CATEGORIA' AS nivel,
                        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') AS key,
                        SUM(existencia) AS existencia_tienda
                    FROM {tabla_inventario}
                    JOIN producto ON producto.id_producto = {tabla_inventario}.id_producto
                    JOIN tienda ON tienda.whscode = {tabla_inventario}.whscode
                    JOIN marca ON marca.id_marca = tienda.id_marca
                    CROSS JOIN variables
                    WHERE {where_clause} {filtro_fecha}
                    GROUP BY producto.departamento, producto.categoria

                    UNION ALL

                    SELECT 
                        'SUBCATEGORIA' AS nivel,
                        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') || '-' || COALESCE(subcategoria, 'SIN SUBCATEGORIA') AS key,
                        SUM(existencia) AS existencia_tienda
                    FROM  {tabla_inventario}
                    JOIN producto ON producto.id_producto =  {tabla_inventario}.id_producto
                    JOIN tienda ON tienda.whscode =  {tabla_inventario}.whscode
                    JOIN marca ON marca.id_marca = tienda.id_marca
                    CROSS JOIN variables
                    WHERE {where_clause} {filtro_fecha}
                    GROUP BY producto.departamento, producto.categoria, producto.subcategoria
                ),
                InventarioCierreAnioAnteriorPN AS (
                    SELECT 
                        'DEPARTAMENTO' AS nivel,
                        departamento AS key,
                        SUM(existencia) AS existencia_tienda_cm_aant
                    FROM historial_inventario_tienda
                    JOIN producto ON producto.id_producto = historial_inventario_tienda.id_producto
                    JOIN tienda ON tienda.whscode = historial_inventario_tienda.whscode
                    JOIN marca ON marca.id_marca = tienda.id_marca
                    CROSS JOIN variables
                    WHERE {where_clause} AND historial_inventario_tienda.fecha = variables.fecha_cierre_anio_anterior
                    GROUP BY producto.departamento

                    UNION ALL

                    SELECT 
                        'CATEGORIA' AS nivel,
                        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') AS key,
                        SUM(existencia) AS existencia_tienda
                    FROM historial_inventario_tienda
                    JOIN producto ON producto.id_producto = historial_inventario_tienda.id_producto
                    JOIN tienda ON tienda.whscode = historial_inventario_tienda.whscode
                    JOIN marca ON marca.id_marca = tienda.id_marca
                    CROSS JOIN variables
                    WHERE {where_clause} AND historial_inventario_tienda.fecha = variables.fecha_cierre_anio_anterior
                    GROUP BY producto.departamento, producto.categoria

                    UNION ALL

                    SELECT 
                        'SUBCATEGORIA' AS nivel,
                        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') || '-' || COALESCE(subcategoria, 'SIN SUBCATEGORIA') AS key,
                        SUM(existencia) AS existencia_tienda
                    FROM historial_inventario_tienda
                    JOIN producto ON producto.id_producto = historial_inventario_tienda.id_producto
                    JOIN tienda ON tienda.whscode = historial_inventario_tienda.whscode
                    JOIN marca ON marca.id_marca = tienda.id_marca
                    CROSS JOIN variables
                    WHERE {where_clause} AND historial_inventario_tienda.fecha = variables.fecha_cierre_anio_anterior
                    GROUP BY producto.departamento, producto.categoria, producto.subcategoria
                ),
                VentasTotalesPorTienda AS (
                    SELECT 
                        SUM(venta.cantidad) AS total_ventas_tienda_pzs,
                        SUM(venta.venta_neta_con_iva) AS total_ventas_efectivo
                    FROM venta
                    JOIN tienda ON tienda.whscode = venta.whscode
                    JOIN marca ON marca.id_marca = tienda.id_marca
                    CROSS JOIN variables
                    WHERE {where_clause} AND venta.fecha BETWEEN variables.fecha_inicio_anio_actual AND variables.fecha_fin_anio_actual
                ),
                InventariosTotalesPorTienda AS (
                    SELECT 
                        SUM({tabla_inventario}.existencia) AS total_existencias_tiendas
                    FROM {tabla_inventario}
                    JOIN tienda ON tienda.whscode = {tabla_inventario}.whscode
                    JOIN marca ON marca.id_marca = tienda.id_marca
                    CROSS JOIN variables
                    WHERE {where_clause} {filtro_fecha}
                ),
                VentaPromedioPorNiveles AS (
                    SELECT 
                        producto.departamento,
                        producto.categoria,
                        producto.subcategoria,
                        COUNT(DISTINCT DATE_TRUNC('month', venta.fecha)) AS meses_con_venta,
                        SUM(CAST(venta.cantidad AS DECIMAL)) AS total_ventas,
                        COALESCE(
                        SUM(CAST(venta.cantidad AS DECIMAL)) / COUNT(DISTINCT DATE_TRUNC('month', venta.fecha)),
                            0
                        ) AS venta_promedio
                    FROM venta
                    JOIN producto ON producto.id_producto = venta.id_producto
                    JOIN tienda ON venta.whscode = tienda.whscode
                    JOIN marca ON tienda.id_marca = marca.id_marca
                    CROSS JOIN variables
                    WHERE {where_clause}
                        AND venta.fecha >= DATE_TRUNC('month', variables.fecha_inicio_periodo_ventas_prom)
                        AND venta.fecha <= variables.fecha_fin_periodo_ventas_prom
                        AND (venta.venta_neta_sin_iva < -1 OR venta.venta_neta_sin_iva > 1)
                    GROUP BY producto.departamento, producto.categoria, producto.subcategoria
                ),
                TotalesPromedioVentaPorNivel AS (
                    SELECT 
                        departamento AS key,
                        MAX(meses_con_venta) AS meses_con_venta, -- Garantiza que no sumemos meses indebidamente
                        SUM(total_ventas) AS total_ventas,
                        COALESCE(SUM(total_ventas) / MAX(meses_con_venta), 0) AS venta_promedio
                    FROM VentaPromedioPorNiveles
                    GROUP BY departamento
                
                    UNION ALL
                
                    SELECT 
                        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') AS key,
                        MAX(meses_con_venta) AS meses_con_venta, -- Máximo por categoría, no sumamos meses
                        SUM(total_ventas) AS total_ventas,
                        COALESCE(SUM(total_ventas) / MAX(meses_con_venta), 0) AS venta_promedio
                    FROM VentaPromedioPorNiveles
                    GROUP BY departamento, categoria
                
                    UNION ALL
                
                    SELECT 
                        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') || '-' || COALESCE(subcategoria, 'SIN SUBCATEGORIA') AS key,
                        meses_con_venta,
                        total_ventas,
                        venta_promedio
                    FROM VentaPromedioPorNiveles
                ),
                PresupuestosTiendas AS (
                    SELECT DISTINCT ON (presupuesto_tienda.whscode) venta_objetivo
                    FROM presupuesto_tienda
                    JOIN tienda ON tienda.whscode = presupuesto_tienda.whscode
                    JOIN marca ON tienda.id_marca = marca.id_marca 
                    CROSS JOIN variables
                    WHERE {where_clause}
                    ORDER BY presupuesto_tienda.whscode, fecha DESC
                ),
                SumaPresupuestosTiendas as (
                    SELECT sum(venta_objetivo) AS venta_objetivo
                    FROM PresupuestosTiendas
                )
                SELECT 
                    COALESCE(t.nivel, i.nivel) AS nivel,
                    COALESCE(t.key, i.key) AS key,
                    COALESCE(t.nombre, 'SIN NOMBRE') AS nombre,
                    COALESCE(t.iva_anio_anterior, 0) AS iva_anio_anterior,
                    COALESCE(t.iva_anio_actual, 0) AS iva_anio_actual,
                    COALESCE(t.variacion_efectivo, 0) AS variacion_efectivo,
                    COALESCE(t.variacion_porcentaje, 0) AS variacion_porcentaje,
                    COALESCE(t.cantidad_anio_anterior, 0) AS cantidad_anio_anterior,
                    COALESCE(t.cantidad_anio_actual, 0) AS cantidad_anio_actual,
                    COALESCE(t.variacion_mes_cantidad, 0) AS variacion_mes_cantidad,
                    COALESCE(t.variacion_mes_porcentaje_cantidad, 0) AS variacion_mes_porcentaje_cantidad,
                    ROUND(COALESCE(t.cantidad_anio_actual, 0) / NULLIF(vt.total_ventas_tienda_pzs, 0), 2) AS porcentaje_participacion_venta_pzs,
                    ROUND(COALESCE(CAST(existencia_tienda AS numeric) / NULLIF(CAST(total_existencias_tiendas AS numeric), 0), 0), 2) AS porcentaje_participacion_inv_pzs,
                    ROUND(COALESCE(t.iva_anio_actual, 0) / NULLIF(vt.total_ventas_efectivo, 0) * spt.venta_objetivo, 2) AS presupuesto,
                    COALESCE(h.existencia_tienda_cm_aant, 0) AS existencia_tienda_cierre_aant,
                    COALESCE(i.existencia_tienda, 0) AS existencia_tienda,
                    ROUND(COALESCE((CAST(i.existencia_tienda AS numeric) / NULLIF(CAST(h.existencia_tienda_cm_aant AS numeric), 0)) - 1, 0), 2) AS variacion_prc_inv,
                    ROUND(COALESCE((CAST(i.existencia_tienda AS numeric) / NULLIF(CAST(pv.venta_promedio AS numeric), 0)), 0), 2) AS mos_tienda

                FROM TotalesVariacionesPorNivel t
                FULL OUTER JOIN InventarioTiendaPorNivel i ON t.nivel = i.nivel AND t.key = i.key
                FULL OUTER JOIN InventarioCierreAnioAnteriorPN h ON t.nivel = h.nivel AND t.key = h.key
                FULL OUTER JOIN TotalesPromedioVentaPorNivel pv ON t.key = pv.key
                CROSS JOIN VentasTotalesPorTienda vt
                CROSS JOIN InventariosTotalesPorTienda invtot
                CROSS JOIN SumaPresupuestosTiendas spt
                WHERE NOT (cantidad_anio_actual<= 0 AND i.existencia_tienda<= 0)
                ORDER BY iva_anio_actual DESC;
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