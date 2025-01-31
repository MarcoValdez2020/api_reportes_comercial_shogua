import pandas as pd
import calendar
from datetime import timedelta, date
from typing import List, Tuple
from datetime import datetime, date
from fastapi import  HTTPException
from typing import List, TypeVar
from sqlmodel import SQLModel
from core.unit_of_work import UnitOfWork
from ventas.venta_schemas import Venta
from shared.shared_service import SharedService
from shared.shared_schemas import Tienda




# Definimos la calse servicio, que sera la encargada de gestionar la logica de negocio, haciendo llamadas al repositorio
class VentaService:
    def __init__(self):
        # Si necesitamos inyecciones de dependencias las pasamos en el constructor :)
        pass
        
    
    def get_all_sales_by_brand(self,nombre_marca:str, fecha_inicio:date, fecha_fin:date) -> List[Venta]:
        """Obtener todas las ventas de una marca en un rago de fechas de la base de datos."""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                return uow.venta_repository.get_all_by_brand_name(nombre_marca, fecha_inicio, fecha_fin)
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener registros")
            
    def get_sales_grouped_by_month_and_whscode(self,nombre_marca:str, fecha_inicio:date, fecha_fin:date) -> List[Tuple[str, str, float]]:
        """Obtener todas las ventas de una marca agrupadas por tienda, año mes en un rago de fechas de la base de datos."""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                return uow.venta_repository.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio, fecha_fin)
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener registros")


    def get_sales_grouped_by_year_and_whscode(self,nombre_marca:str, fecha_inicio:date, fecha_fin:date) -> List[Tuple[str, str, float]]:
        """Obtener todas las ventas de una marca agrupadas por tienda y año en un rago de fechas de la base de datos."""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                return uow.venta_repository.get_sales_grouped_by_year_and_whscode(nombre_marca, fecha_inicio, fecha_fin)
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener registros")
    

    def get_ventas_promedio_mensual_por_marca(self, nombre_marca: str,fecha_inicio:date, fecha_fin: date) -> list: 
        """Obtener las ventas promedio por tienda en un rago de fechas de la base de datos."""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                return uow.venta_repository.get_ventas_promedio_mensual_por_marca(nombre_marca, fecha_inicio, fecha_fin)
            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener registros")


    #? Funciones de transformacion de datos

    def transform_sales_gropued_by_month(self, ventas_mes:List[Tuple[str, str, float]]):
            """Método para transformar de lista de tupla a lista de diccionarios"""
            result_dict = []
            
            for record in ventas_mes:
                # La tupla tiene el siguiente formato: (mes, whscode, total_venta_neta_con_iva)
                mes = record[0].strftime('%Y-%m-%d')  # Formateamos la fecha en 'yyyy-mm-dd'
                whscode = record[1]  # Código del almacén
                total_venta_neta_con_iva = record[2]  # Convertimos el total de Decimal a float
                
                # Creamos el diccionario con el formato deseado
                result_dict.append({
                    'mes': mes,
                    'whscode': whscode,
                    'total_venta_neta_con_iva': float(total_venta_neta_con_iva)
                })

            return result_dict


    def transform_sales_gropued_by_year(self, ventas_anio:List[Tuple[str, str, float]]):
            """Método para transformar de lista de tupla a lista de diccionarios"""
            result_dict = []
            
            for record in ventas_anio:
                # La tupla tiene el siguiente formato: (mes, whscode, total_venta_neta_con_iva)
                anio = record[0].strftime('%Y-%m-%d')  # Formateamos la fecha en 'yyyy-mm-dd'
                whscode = record[1]  # Código del almacén
                total_venta_neta_con_iva = record[2]  # Convertimos el total de Decimal a float
                
                # Creamos el diccionario con el formato deseado
                result_dict.append({
                    'anio': anio,
                    'whscode': whscode,
                    'total_venta_neta_con_iva': float(total_venta_neta_con_iva)
                })

            return result_dict


    def get_first_and_last_sale_date_by_brand_name(self,nombre_marca:str):
            """Obtener la primera y ultima fecha de ventas de una marca"""
            with UnitOfWork() as uow:
                # Intenta la operacion en la bd
                try:
                    fechas_df =  uow.venta_repository.get_first_and_last_sale_date_by_brand_name(nombre_marca)
                    return fechas_df.to_dict('records')
                except Exception as e:
                        # Log de la excepción para saber el error
                        print(f"Error al obtener registros: {e}")

    def getAvailableSalesYears(self, nombre_marca:str) -> List[int]:
        """Obtener los años en que una marca tiene ventas registradas"""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                data = uow.venta_repository.get_years_with_sales_by_brand_name(nombre_marca)
                # Diccionario para convertir números de mes a nombres de mes
                meses_dict = {
                    1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril', 5: 'Mayo', 6: 'Junio',
                    7: 'Julio', 8: 'Agosto', 9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
                }

                # Convertir los números de mes a nombres de mes y estructurar la respuesta
                response = []
                for anio, meses in data:
                    # Convertir los números de mes flotantes a enteros y luego a nombres
                    meses_con_nombres = [{'index':int(mes),'name': meses_dict.get(int(mes))} for mes in set(meses)]
                    response.append({
                        'anio': int(anio),
                        'meses_venta': meses_con_nombres
                    })
                
                return response

            except Exception as e:
                    # Log de la excepción para saber el error
                    print(f"Error al obtener registros: {e}")
                    raise HTTPException(status_code=500, detail="Error al obtener registros")


    #? Funciones para trabajar con datos

    def get_hierarchical_sales_report(self, nombre_marca: str, whscodes: list[str], fecha_inicio_mes_anio_actual: str, fecha_fin_mes_anio_actual: str, 
                                fecha_inicio_mes_anio_anterior: str, fecha_fin_mes_anio_anterior: str,tipo_inventario:str, tallas: list[str] = None,
                                generos: list[str] = None, disenios: list[str] = None, colecciones: list[str] = None):
        """Funcion para traer el reporte de detalle venta"""
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                # Obtener las fechas de inicio y fin del periodo de ventas para calcular el promedio de ventas por niveles
                fecha_inicio_periodo_ventas_prom, fecha_fin_periodo_ventas_prom = self.obtener_fecha_fin_y_fecha_inicio_para_promedio_ventas_por_niveles(fecha_fin_mes_anio_actual)

                # Hacemos la peticion al repositorio
                data =  uow.venta_repository.get_hierarchical_sales_report(
                    nombre_marca, whscodes, fecha_inicio_mes_anio_actual, fecha_fin_mes_anio_actual, 
                    fecha_inicio_mes_anio_anterior, fecha_fin_mes_anio_anterior, tipo_inventario, 
                    fecha_inicio_periodo_ventas_prom, fecha_fin_periodo_ventas_prom,
                    tallas=tallas, generos=generos, disenios=disenios, colecciones=colecciones
                )

                resultado = []

                # Comparar la marca recibida
                if nombre_marca == 'AY GÜEY' or nombre_marca == 'MUMUSO':
                    # Si la marca es AG o MUMUSO entonces tiene departamento
                    # Mapa para almacenar las categorías y subcategorías
                    departamentos = {}
                    

                    # Primero, creamos los departamentos
                    for row in data:
                        nivel, key, nombre, venta_neta_con_iva_anio_anterior, venta_neta_con_iva_anio_actual, variacion_mes_efectivo, variacion_mes_porcentaje, venta_cantidad_anio_anterior, venta_cantidad_anio_actual, variacion_mes_cantidad, variacion_mes_porcentaje_cantidad, porcentaje_participacion_venta_pzs, porcentaje_participacion_inv_pzs, presupuesto, existencia_tienda_cierre_aant, existencia_tienda, variacion_prc_inv, mos_tienda = row


                        # Asignar 0 a existencia si es None o NULL
                        existencia_tienda = existencia_tienda if existencia_tienda is not None else 0

                        if nivel == "DEPARTAMENTO":
                            # Si el departamento no existe en el mapa, lo creamos
                            if key not in departamentos:
                                departamentos[key] = {
                                    "nivel": "DEPARTAMENTO",
                                    "key": key,
                                    "data": {
                                        "nombre": nombre,
                                        "venta_neta_con_iva_anio_anterior": venta_neta_con_iva_anio_anterior,
                                        "venta_neta_con_iva_anio_actual": venta_neta_con_iva_anio_actual,
                                        "variacion_mes_efectivo": variacion_mes_efectivo,
                                        "variacion_mes_porcentaje": variacion_mes_porcentaje,
                                        "venta_cantidad_anio_anterior": venta_cantidad_anio_anterior,
                                        "venta_cantidad_anio_actual": venta_cantidad_anio_actual,
                                        "variacion_mes_cantidad": variacion_mes_cantidad,
                                        "variacion_mes_porcentaje_cantidad": variacion_mes_porcentaje_cantidad,
                                        "porcentaje_participacion_venta_pzs": porcentaje_participacion_venta_pzs,
                                        "porcentaje_participacion_inv_pzs": porcentaje_participacion_inv_pzs,
                                        "presupuesto": presupuesto,
                                        "existencia_tienda_cierre_aant": existencia_tienda_cierre_aant,
                                        "existencia_tienda": existencia_tienda,
                                        "variacion_prc_inv": variacion_prc_inv,
                                        "mos_tienda": mos_tienda
                                    },
                                    "children": []
                                }
                    
                    # Mapa para almacenar categorías
                    categorias = {}

                    # Ahora agregamos las categorías y subcategorías
                    for row in data:
                        nivel, key, nombre, venta_neta_con_iva_anio_anterior, venta_neta_con_iva_anio_actual, variacion_mes_efectivo, variacion_mes_porcentaje, venta_cantidad_anio_anterior, venta_cantidad_anio_actual, variacion_mes_cantidad, variacion_mes_porcentaje_cantidad, porcentaje_participacion_venta_pzs, porcentaje_participacion_inv_pzs, presupuesto, existencia_tienda_cierre_aant, existencia_tienda, variacion_prc_inv, mos_tienda = row

                        # Asignar 0 a existencia si es None o NULL
                        existencia_tienda = existencia_tienda if existencia_tienda is not None else 0
                    
                        # Creando el diccionario para cada nivel
                        item = {
                            "nivel": nivel,
                            "key": key,
                            "data": {
                                "nombre": nombre,
                                    "venta_neta_con_iva_anio_anterior": venta_neta_con_iva_anio_anterior,
                                    "venta_neta_con_iva_anio_actual": venta_neta_con_iva_anio_actual,
                                    "variacion_mes_efectivo": variacion_mes_efectivo,
                                    "variacion_mes_porcentaje": variacion_mes_porcentaje,
                                    "venta_cantidad_anio_anterior": venta_cantidad_anio_anterior,
                                    "venta_cantidad_anio_actual": venta_cantidad_anio_actual,
                                    "variacion_mes_cantidad": variacion_mes_cantidad,
                                    "variacion_mes_porcentaje_cantidad": variacion_mes_porcentaje_cantidad,
                                    "porcentaje_participacion_venta_pzs": porcentaje_participacion_venta_pzs,
                                    "porcentaje_participacion_inv_pzs": porcentaje_participacion_inv_pzs,
                                    "presupuesto": presupuesto,
                                    "existencia_tienda_cierre_aant": existencia_tienda_cierre_aant,
                                    "existencia_tienda": existencia_tienda,
                                    "variacion_prc_inv": variacion_prc_inv,
                                    "mos_tienda": mos_tienda
                            },
                            "children": []
                        }

                        # Si el nivel es CATEGORIA
                        if nivel == "CATEGORIA":
                            # Encuentra el departamento al que pertenece la categoría
                            departamento_key = key.split("-")[0]
                            departamento = departamentos.get(departamento_key)
                            
                            if departamento:
                                departamento["children"].append(item)
                                categorias[key] = item

                        # Si el nivel es SUBCATEGORIA
                        elif nivel == "SUBCATEGORIA":
                            # Encuentra la categoría padre
                            categoria_key = "-".join(key.split("-")[:2])
                            categoria = categorias.get(categoria_key)
                            
                            if categoria:
                                categoria["children"].append(item)
                    
                    # Convertimos los departamentos en una lista para el resultado final
                    resultado = list(departamentos.values())

                else:

                    # Mapa para almacenar las categorias
                    categorias = {}

                    # Primero, creamos las categorias
                    for row in data:
                        nivel, key, nombre, venta_neta_con_iva_anio_anterior, venta_neta_con_iva_anio_actual, variacion_mes_efectivo, variacion_mes_porcentaje, venta_cantidad_anio_anterior, venta_cantidad_anio_actual, variacion_mes_cantidad, variacion_mes_porcentaje_cantidad, porcentaje_participacion_venta_pzs, porcentaje_participacion_inv_pzs, presupuesto, existencia_tienda_cierre_aant, existencia_tienda, variacion_prc_inv, mos_tienda, existencia_bodega, existencia_almacen_virtual = row
                        
                        # Asignar 0 a existencia si es None o NULL
                        existencia_tienda = existencia_tienda if existencia_tienda is not None else 0
                        
                        if nivel == "CATEGORIA":
                            # Si el departamento no existe en el mapa, lo creamos
                            if key not in categorias:
                                categorias[key] = {
                                    "nivel": "CATEGORIA",
                                    "key": key,
                                    "data": {
                                        "nombre": nombre,
                                        "venta_neta_con_iva_anio_anterior": venta_neta_con_iva_anio_anterior,
                                        "venta_neta_con_iva_anio_actual": venta_neta_con_iva_anio_actual,
                                        "variacion_mes_efectivo": variacion_mes_efectivo,
                                        "variacion_mes_porcentaje": variacion_mes_porcentaje,
                                        "venta_cantidad_anio_anterior": venta_cantidad_anio_anterior,
                                        "venta_cantidad_anio_actual": venta_cantidad_anio_actual,
                                        "variacion_mes_cantidad": variacion_mes_cantidad,
                                        "variacion_mes_porcentaje_cantidad": variacion_mes_porcentaje_cantidad,
                                        "porcentaje_participacion_venta_pzs": porcentaje_participacion_venta_pzs,
                                        "porcentaje_participacion_inv_pzs": porcentaje_participacion_inv_pzs,
                                        "presupuesto": presupuesto,
                                        "existencia_tienda_cierre_aant": existencia_tienda_cierre_aant,
                                        "existencia_tienda": existencia_tienda,
                                        "variacion_prc_inv": variacion_prc_inv,
                                        "mos_tienda": mos_tienda,
                                        "existencia_bodega": existencia_bodega,
                                        "existencia_almacen_virtual": existencia_almacen_virtual
                                    },
                                    "children": []
                                }
                    
                    # Ahora agregamos las categorías y subcategorías
                    

                    subcategorias = {}
                    for row in data:
                        nivel, key, nombre, venta_neta_con_iva_anio_anterior, venta_neta_con_iva_anio_actual, variacion_mes_efectivo, variacion_mes_porcentaje, venta_cantidad_anio_anterior, venta_cantidad_anio_actual, variacion_mes_cantidad, variacion_mes_porcentaje_cantidad, porcentaje_participacion_venta_pzs, porcentaje_participacion_inv_pzs, presupuesto, existencia_tienda_cierre_aant, existencia_tienda, variacion_prc_inv, mos_tienda, existencia_bodega, existencia_almacen_virtual = row
                        
                        # Asignar 0 a existencia si es None o NULL
                        existencia_tienda = existencia_tienda if existencia_tienda is not None else 0
                        
                        # Creando el diccionario para cada nivel
                        item = {
                            "nivel": nivel,
                            "key": key,
                            "data": {
                                "nombre": nombre,
                                "venta_neta_con_iva_anio_anterior": venta_neta_con_iva_anio_anterior,
                                "venta_neta_con_iva_anio_actual": venta_neta_con_iva_anio_actual,
                                "variacion_mes_efectivo": variacion_mes_efectivo,
                                "variacion_mes_porcentaje": variacion_mes_porcentaje,
                                "venta_cantidad_anio_anterior": venta_cantidad_anio_anterior,
                                "venta_cantidad_anio_actual": venta_cantidad_anio_actual,
                                "variacion_mes_cantidad": variacion_mes_cantidad,
                                "variacion_mes_porcentaje_cantidad": variacion_mes_porcentaje_cantidad,
                                "porcentaje_participacion_venta_pzs": porcentaje_participacion_venta_pzs,
                                "porcentaje_participacion_inv_pzs": porcentaje_participacion_inv_pzs,
                                "presupuesto": presupuesto,
                                "existencia_tienda_cierre_aant": existencia_tienda_cierre_aant,
                                "existencia_tienda": existencia_tienda,
                                "variacion_prc_inv": variacion_prc_inv,
                                "mos_tienda": mos_tienda,
                                "existencia_bodega": existencia_bodega,
                                "existencia_almacen_virtual": existencia_almacen_virtual
                            },
                            "children": []
                        }

                        

                        # Si el nivel es CATEGORIA
                        if nivel == "SUBCATEGORIA":
                            # Encuentra la cateogira a la que pertenece la subcategoría
                            categoria_key = key.split("-")[0]
                            categoria = categorias.get(categoria_key)
                            
                            if categoria:
                                categoria["children"].append(item)
                                subcategorias[key] = item

                    # Convertimos las cateogirias en una lista para el resultado final
                    resultado = list(categorias.values())

                return resultado
            except Exception as e:
                # Log de la excepción para saber el error
                print(f"Error al obtener registros: {e}")


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
        """Función para obtener las combinaciones posibles de generos, tiendas y diseños """
        with UnitOfWork() as uow:
            # Intenta la operacion en la bd
            try:
                # Hacemos la peticion al repositorio
                data =  uow.venta_repository.filtrar_campos_detalle_tienda(
                    whscodes, fecha_inicio_mes_anio_actual, fecha_fin_mes_anio_actual, fecha_inicio_mes_anio_anterior, 
                    fecha_fin_mes_anio_anterior, tallas=tallas, generos=generos, disenios=disenios, colecciones=colecciones
                )

                # Claves para el diccionario
                keys = ["talla", "genero", "disenio", "coleccion"]

                # Convertir a lista de diccionarios
                response = [dict(zip(keys, row)) for row in data]

                return response
            except Exception as e:
                # Log de la excepción para saber el error
                print(f"Error al obtener registros: {e}")


    #? Funciones para trabajar con fechas
    def obtener_domingo_anterior(self, fecha:date):
        """Función para obtener la fecha en yyyy-mm-dd del domingo anterior a la fecha recibida"""
        # Convertir la fecha a datetime
        fecha_dt = datetime.combine(fecha, datetime.min.time())
        # Obtener el día de la semana
        dia_semana = fecha_dt.weekday()
        # Restar los días necesarios para llegar al domingo anterior
        fecha_domingo_anterior = fecha_dt - timedelta(days=dia_semana + 1)
    
        return fecha_domingo_anterior.date()
    

    def obtener_fecha_fin_y_fecha_inicio_para_promedio_ventas_por_niveles(self,fecha_fin_mes_anio_actual:str):
        """Funcion para obtener la fecha de inicio y fin del periodo para calcular el promedio de ventas por niveles"""

        # Convertir la fecha de fin a date
        fecha_fin = datetime.strptime(fecha_fin_mes_anio_actual, '%Y-%m-%d').date()

        # Vemos que fecha es hoy (que es la que esta lanzando la peticion)
        hoy =  date.today()

        # Verificamos que la fecha_fin_mes_anio_actual ya haya pasado por medio de la comparacion de fechas
        if fecha_fin > hoy:
            # La fecha de fin aun no ha pasado, entonces retornamos la fecha del domingo anterior a la fecha de hoy
            fecha_fin_periodo_ventas_prom = self.obtener_domingo_anterior(hoy)
        else:
            # La fecha de fin ya paso, entonces se puede usar como fecha de fin del periodo de ventas
            fecha_fin_periodo_ventas_prom = fecha_fin
        
        # Calcular la fecha de inicio del periodo de ventas restando 365 dias a la fecha de fin preiodo ventas
        fecha_inicio_periodo_ventas_prom = fecha_fin_periodo_ventas_prom - pd.DateOffset(days=365)
        # print(f'periodo: {fecha_inicio_periodo_ventas} - {fecha_fin_periodo_ventas}')
        return fecha_inicio_periodo_ventas_prom.strftime('%Y-%m-%d'), fecha_fin_periodo_ventas_prom.strftime('%Y-%m-%d')





