import pandas as pd
from pandas.tseries.offsets import DateOffset
import numpy as np
import calendar
import json
from datetime import datetime, date

from shared.shared_service import SharedService
from ventas.venta_service import VentaService
from inventarios.inventario_service import InventarioService
from reports.report_respones import EndMonthReportAGyMumuso,FinallyEndMonthReportAGyMumuso, StoresEndMonthReportTous, FinallyEndMonthReportTous, FinallyEndMonthReportPenguin, StoresEndMonthReportPenguin


# Definimos la calse servicio, que sera la encargada de gestionar la logica de negocio, haciendo llamadas al repositorio
class ReportService:
    def __init__(self, shared_service: SharedService, venta_service: VentaService, inventario_service: InventarioService):
        self.shared_service = shared_service
        self.venta_service = venta_service
        self.inventario_service = inventario_service
        pass

    # Funcion para obtener el reporte de cierre de mes de Ay Güey y mumuso
    def get_month_end_report_ag_y_mu(self,nombre_marca:str, mes:str, anio:int, tipo_inventario:str):
        """Metodo para obtener los datos de cierre de mes de las marcas Ay Güey y Mumuso"""

        # Obtener registros de las tiendas por marca y transformalo en df
        tiendas = [t.to_dict() for t in self.shared_service.get_all_stores_by_brand_name(nombre_marca)]
        tiendas_df = pd.DataFrame(tiendas)

        # Obtener los inventarios de todas las tiendas
        # Si el tipo de inventario es 'actual', entonces traemos los inventario de tiendas actuales, pero si es 'cierre-mes' entonces llamamos al historial del inventario del mes seleccionado
        if tipo_inventario == 'actual':
            inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name(nombre_marca)
        elif tipo_inventario == 'cierre-mes':
            # Evaluamos que se tenga el inventario con la fecha especificada, si no se retorno nada entonces usar el cierre de mes mas cercano
            lista_historiales_inventarios = self.inventario_service.get_end_month_history_inventory_list(nombre_marca)
            # Buscamos el año y mes dentro del diccionadio de lista_historiales_inventarios
            resultado = None
            for item in lista_historiales_inventarios:
                if item['anio'] == anio:  # Buscamos el año
                    for inventario in item['inventario_cierre']:
                        if inventario['name'] == mes.lower():  # Busca el mes dentro de 'inventario_cierre'
                            resultado = inventario
                            break
                    if resultado:
                        break

            if resultado:
                print("Elemento encontrado:", resultado)
                inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name_and_month(nombre_marca, mes, anio)

            else:
                print("No se encontró el mes en el año especificado.")
                # Si no se encuentra el mes en el año especificado, entonces se obtiene el inventario actual
                # TODO: Cambiar esto en un futuro no muy lejano por el inventario mas cercano al del cierre de ese mes
                inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name(nombre_marca)
                # Crear una nueva lista con los valores numéricos reemplazados por 0 en las existencias
                inventarios_tiendas = [(key, 0) for key, _ in inventarios_tiendas]

        else:
            raise ValueError("El tipo de inventario no es válido")
        
        inventarios_tiendas_dict = self.inventario_service.transform_store_inventories_total_stock(inventarios_tiendas)
        inventarios_tiendas_df = pd.DataFrame(inventarios_tiendas_dict)


        # Obtener todas las ventas de inicio a fin del mes seleccionado
        if anio:
            anio_actual = anio
        else:
            anio_actual = date.today().year
    
        anio_anterior = anio_actual-1

        meses_dict = {
                'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
                'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
                'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
            }
        
        mes = mes.lower()
        mes_numero = meses_dict[mes]
        _,ultimo_dia_mes_anio_actual = calendar.monthrange(anio_actual,mes_numero)
        _,ultimo_dia_mes_anio_anterior = calendar.monthrange(anio_anterior,mes_numero)

        # Definimos las variables para el fin de mes del año actual y el anterior
        fecha_inicio_mes_anio_actual = date(anio_actual,mes_numero,1)
        fecha_fin_mes_anio_actual = date(anio_actual,mes_numero,ultimo_dia_mes_anio_actual)

        fecha_fin_mes_anio_anterior = date(anio_anterior,mes_numero,ultimo_dia_mes_anio_anterior)
        fecha_inicio_mes_anio_anterior = date(anio_anterior,mes_numero,1)
        
        # Obtener las ventas solamente del mes pero en el año actual de inicio a fin
        ventas_mes_anio_actual= self.venta_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio_mes_anio_actual, fecha_fin_mes_anio_actual)
        ventas_mes_dict_anio_actual = self.venta_service.transform_sales_gropued_by_month(ventas_mes_anio_actual)
        ventas_mes_actual_df = pd.DataFrame(ventas_mes_dict_anio_actual)


        # Obtener las ventas solamente del mes pero del año anterior
        ventas_mes_anio_anterior= self.venta_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio_mes_anio_anterior, fecha_fin_mes_anio_anterior)
        ventas_mes_dict_anio_anterior = self.venta_service.transform_sales_gropued_by_month(ventas_mes_anio_anterior)
        ventas_mes_anterior_df = pd.DataFrame(ventas_mes_dict_anio_anterior)

        #* Calculos del YTD
        # Obtener todas las ventas de inicio de año hasta el fin del mes seleccionado
        fecha_inicio_anio_actual = date(anio_actual,1,1)

        fecha_inicio_anio_anterior = date(anio_anterior,1,1)

        # Obtenemos el ytd del año anterior
        ventas_ytd_anio_anterior = self.venta_service.get_sales_grouped_by_year_and_whscode(nombre_marca,fecha_inicio_anio_anterior, fecha_fin_mes_anio_anterior)
        ventas_ytd_dict_anio_anterior = self.venta_service.transform_sales_gropued_by_year(ventas_ytd_anio_anterior)
        ventas_ytd_anio_anterior_df = pd.DataFrame(ventas_ytd_dict_anio_anterior)
        
        # Obtenemos el ytd del año actual
        ventas_ytd_anio_actual = self.venta_service.get_sales_grouped_by_year_and_whscode(nombre_marca,fecha_inicio_anio_actual, fecha_fin_mes_anio_actual)
        ventas_ytd_dict_anio_actual = self.venta_service.transform_sales_gropued_by_year(ventas_ytd_anio_actual)
        ventas_ytd_anio_actual = pd.DataFrame(ventas_ytd_dict_anio_actual)

        # Obtenemos las ventas promedio de cada tienda
        ventas_promedio_df = self.calcularVentaPromedio(nombre_marca,fecha_fin_mes_anio_actual)

        # Obtenemos los presupuestos de cada tienda
        presupuestos_tiendas = self.shared_service.get_all_store_budgets()
        # Transformamos a una lista de diccionarios los presupuestos tiendas
        presupuestos_tiendas_dict_list = [presupuesto.to_dict() for presupuesto in presupuestos_tiendas]
        presupuestos_tiendas_df = pd.DataFrame(presupuestos_tiendas_dict_list)
        
        reporte_cierre_mes = self.fusionar_dataframes_cierre_mes_ag_y_mu(ventas_mes_anterior_df,ventas_mes_actual_df,
                                                                        ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual,
                                                                        tiendas_df,inventarios_tiendas_df, ventas_promedio_df, presupuestos_tiendas_df)
            
        return reporte_cierre_mes
    

    # Funcion para concatenar el df de cierre de mes ag y mumuso
    def fusionar_dataframes_cierre_mes_ag_y_mu(self, ventas_mes_anterior_df:pd.DataFrame, ventas_mes_actual_df:pd.DataFrame,
                                                ventas_ytd_anio_anterior_df:pd.DataFrame,ventas_ytd_anio_actual:pd.DataFrame,
                                                tiendas_df:pd.DataFrame, inventarios_tiendas_df:pd.DataFrame, ventas_promedio_df: pd.DataFrame,
                                                presupuestos_tiendas_df:pd.DataFrame):
        
        #Renombrar columnas de cada df
        ventas_mes_anterior_df.rename(columns={"total_venta_neta_con_iva": "venta_mensual_anio_anterior_iva"}, inplace=True)
        ventas_mes_actual_df.rename(columns={"total_venta_neta_con_iva": "venta_mensual_anio_actual_iva"}, inplace=True)
        ventas_ytd_anio_anterior_df.rename(columns={"total_venta_neta_con_iva": "ytd_anio_anterior_iva"}, inplace=True)
        ventas_ytd_anio_actual.rename(columns={"total_venta_neta_con_iva": "ytd_anio_actual_iva"}, inplace=True)

        # Eliminar columnas insecesrias
        ventas_mes_anterior_df.drop(columns=['mes'], inplace=True)
        ventas_mes_actual_df.drop(columns=['mes'], inplace=True)
        ventas_ytd_anio_anterior_df.drop(columns=['anio'], inplace=True)
        ventas_ytd_anio_actual.drop(columns=['anio'], inplace=True)
        presupuestos_tiendas_df.drop(columns=['fecha','id_presupuesto_tienda'], inplace=True)

        # Fusionar mes con mes
        ventas_mes_df = pd.merge(ventas_mes_anterior_df,ventas_mes_actual_df, on='whscode', how='outer')

        # Fusionar año con año
        ventas_ytd_df = pd.merge(ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual, on='whscode', how='outer')

        # Fusionar meses con el ytd
        ventas_cierre_mes_df = pd.merge(ventas_mes_df,ventas_ytd_df, on='whscode', how='outer')

        # Fusionar con presupuesto de tienda
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df, presupuestos_tiendas_df, on='whscode', how='left')


        # Calcular las variaciones
        ventas_cierre_mes_df = ventas_cierre_mes_df.fillna(0)
        
        ventas_cierre_mes_df['variacion_mes_porcentaje'] = np.where(ventas_cierre_mes_df['venta_mensual_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] / ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_mes_efectivo'] =  ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] - ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']

        ventas_cierre_mes_df['variacion_ytd_porcentaje'] = np.where(ventas_cierre_mes_df['ytd_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['ytd_anio_actual_iva'] / ventas_cierre_mes_df['ytd_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_ytd_efectivo']  =   ventas_cierre_mes_df['ytd_anio_actual_iva'] - ventas_cierre_mes_df['ytd_anio_anterior_iva']


        ventas_cierre_mes_df['variacion_vta_obj_porcentaje'] = np.where(ventas_cierre_mes_df['venta_objetivo'] != 0,
                                            (((ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] / ventas_cierre_mes_df['venta_objetivo']) - 1))*100,0)
        

        # Hacer el merge con los inventarios
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,inventarios_tiendas_df, on='whscode', how='outer')

        # Hacemos el merge con las ventas promedio
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,ventas_promedio_df, on='whscode', how='outer')

        # Calculamos el mos
        ventas_cierre_mes_df['mos'] = ventas_cierre_mes_df['existencia'] / ventas_cierre_mes_df['venta_promedio'].astype(float)

        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,tiendas_df[['whscode','nombre_sucursal','tipo_tienda','ciudad','estado_operativo','comparabilidad']],on='whscode',how='left')

        # Llenamos con 0 las existencias que no tengan coincidencia
        ventas_cierre_mes_df= ventas_cierre_mes_df.fillna(0)

        # print(ventas_cierre_mes_df)
        # Crear una lista de objetos EndMonthReportAGyMumuso a partir del DataFrame
        list_of_reports = [
            EndMonthReportAGyMumuso(**row.to_dict()) for _, row in ventas_cierre_mes_df.iterrows()
        ]

        # Formar el totales del reporte final
        total_venta_mensual_anio_anterior_iva = ventas_cierre_mes_df['venta_mensual_anio_anterior_iva'].sum()
        total_venta_mensual_anio_actual_iva = ventas_cierre_mes_df['venta_mensual_anio_actual_iva'].sum()
        total_variacion_mes_porcentaje = ((total_venta_mensual_anio_actual_iva / total_venta_mensual_anio_anterior_iva)-1)*100
        total_variacion_mes_efectivo = total_venta_mensual_anio_actual_iva - total_venta_mensual_anio_anterior_iva

        total_ytd_anio_anterior_iva = ventas_cierre_mes_df['ytd_anio_anterior_iva'].sum()
        total_ytd_anio_actual_iva = ventas_cierre_mes_df['ytd_anio_actual_iva'].sum()
        total_variacion_ytd_porcentaje = ((total_ytd_anio_actual_iva / total_ytd_anio_anterior_iva)-1)*100
        total_variacion_ytd_efectivo = total_ytd_anio_actual_iva - total_ytd_anio_anterior_iva

        total_venta_objetivo = ventas_cierre_mes_df['venta_objetivo'].sum()
        if total_venta_objetivo == 0:
            total_variacion_vta_obj_porcentaje = 0
        else:
            total_variacion_vta_obj_porcentaje = ((total_venta_mensual_anio_actual_iva / total_venta_objetivo)-1)*100
    
        total_punto_equilibrio = ventas_cierre_mes_df['punto_equilibrio'].sum()

        total_existencia = ventas_cierre_mes_df['existencia'].sum()
        total_venta_promedio = float(ventas_cierre_mes_df['venta_promedio'].sum())
        total_mos = total_existencia/total_venta_promedio


        reporte_final_cierre_mes = FinallyEndMonthReportAGyMumuso(
            stores=list_of_reports,
            total_venta_mensual_anio_anterior_iva=total_venta_mensual_anio_anterior_iva,
            total_venta_mensual_anio_actual_iva=total_venta_mensual_anio_actual_iva,
            total_variacion_mes_porcentaje=total_variacion_mes_porcentaje,
            total_variacion_mes_efectivo=total_variacion_mes_efectivo,
            total_ytd_anio_anterior_iva=total_ytd_anio_anterior_iva,
            total_ytd_anio_actual_iva=total_ytd_anio_actual_iva,
            total_variacion_ytd_porcentaje=total_variacion_ytd_porcentaje,
            total_variacion_ytd_efectivo=total_variacion_ytd_efectivo,
            total_venta_objetivo= total_venta_objetivo,
            total_variacion_vta_obj_porcentaje= total_variacion_vta_obj_porcentaje,
            total_punto_equilibrio= total_punto_equilibrio,
            total_existencia=total_existencia,
            total_mos=total_mos
        )
        
        return reporte_final_cierre_mes
    

    # Funcion para calcular el promedo de venta de todas las marcas (en cantidad)
    def calcularVentaPromedio(self, nombre_marca:str, fecha_fin:date):
        mes_num = fecha_fin.month
        anio_fecha_fin = fecha_fin.year
        # Calculamos la fecha de inicio de la consulta que es 11 meses atras
        fecha_inicio = datetime(anio_fecha_fin,mes_num,1) - DateOffset(months=11) 
        # print(f'****************fecha de inicio: {fecha_inicio}')

        ventas_promedio = self.venta_service.get_ventas_promedio_mensual_por_marca(nombre_marca,fecha_inicio,fecha_fin)
        ventas_promedio_df = pd.DataFrame(ventas_promedio)
        return ventas_promedio_df
    

    # Funcion para obtener el reporte de cierre de mes de tous
    def get_month_end_report_tous_hm_y_ricodeli(self, nombre_marca:str, mes:str, anio:int, tipo_inventario:str):
        """Metodo para obtener los datos de cierre de mes de Tous Hetal Mevy y Ricodeli"""
        # print(f'mes: {mes}')
        # Obtener registros de las tiendas por marca y transformalo en df
        tiendas = [t.to_dict() for t in self.shared_service.get_all_stores_by_brand_name(nombre_marca)]
        tiendas_df = pd.DataFrame(tiendas)

        # Obtener los inventarios de todas las tiendas
        # Si el tipo de inventario es 'actual', entonces traemos los inventario de tiendas actuales, pero si es 'cierre-mes' entonces llamamos al historial del inventario del mes seleccionado
        if tipo_inventario == 'actual':
            # Si el tipo de peticion es con inventario actual obtenemos el inventario de tiendas
            inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name(nombre_marca)
            # Luego obtenemos el de los almacenes
            # Obtener la suma de stock de los almacenes virtuales
            inventarios_almacenes_virtuales = self.inventario_service.get_virtual_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
            suma_stock_almacenes_virtuales = inventarios_almacenes_virtuales[0]

            # Obtener la suma de los inventarios fisicos de los almacenes
            inventarios_almacenes_fisicos = self.inventario_service.get_physical_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
        
        elif tipo_inventario == 'cierre-mes':
            # Evaluamos que se tenga el inventario con la fecha especificada, si no se retorno nada entonces usar el cierre de mes mas cercano
            lista_historiales_inventarios = self.inventario_service.get_end_month_history_inventory_list(nombre_marca)
            # Buscamos el año y mes dentro del diccionadio de lista_historiales_inventarios
            resultado = None
            for item in lista_historiales_inventarios:
                if item['anio'] == anio:  # Buscamos el año
                    for inventario in item['inventario_cierre']:
                        if inventario['name'] == mes.lower():  # Busca el mes dentro de 'inventario_cierre'
                            resultado = inventario
                            break
                    if resultado:
                        break

            if resultado:
                # Si existe el mes entonces cargamos el inventario desde el historial de inventarios
                inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name_and_month(nombre_marca, mes, anio)
                # Si existe entonces cargamos la suma de inventarios del historial de almacenes fisicos
                # Obtener la suma de stock de los almacenes virtuales
                inventarios_almacenes_virtuales = self.inventario_service.get_history_from_virtual_warehouses_inventories_total_stock_by_brand_name(nombre_marca, mes, anio)
                suma_stock_almacenes_virtuales = int(inventarios_almacenes_virtuales)
                # Obtener la suma de los inventarios fisicos de los almacenes
                inventarios_almacenes_fisicos = self.inventario_service.get_history_from_physical_warehouses_inventories_total_stock_by_brand_name(nombre_marca, mes, anio)


            else:
                print("No se encontró el mes en el año especificado.")
                # Si no se encuentra el mes en el año especificado, entonces se obtiene el inventario actual para las tiendas
                inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name(nombre_marca)
                # Crear una nueva lista con los valores numéricos reemplazados por 0 en las existencias
                inventarios_tiendas = [(key, 0) for key, _ in inventarios_tiendas]

                # De igual manera setear en 0 la suma de existencias virtuales
                suma_stock_almacenes_virtuales = 0
                
                # Extraemos los actuales para la estuctura y los seteamos en 0
                inventarios_almacenes_fisicos = self.inventario_service.get_physical_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
                # Crear una nueva lista con el las existencias en 0
                inventarios_almacenes_fisicos = [(x, y, 0) for x, y, _ in inventarios_almacenes_fisicos]
                
        else:
            raise ValueError("El tipo de inventario no es válido")
        
        inventarios_tiendas_dict = self.inventario_service.transform_store_inventories_total_stock(inventarios_tiendas)
        inventarios_tiendas_df = pd.DataFrame(inventarios_tiendas_dict)

        # # Obtener la suma de stock de los almacenes virtuales
        # inventarios_almacenes_virtuales = self.inventario_service.get_virtual_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
        # suma_stock_almacenes_virtuales = inventarios_almacenes_virtuales[0]

        # Obtener la suma de los inventarios fisicos de los almacenes
        # inventarios_almacenes_fisicos = self.inventario_service.get_physical_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
        # print(inventarios_almacenes_fisicos, len(inventarios_almacenes_fisicos))
        
        # En caso de no tener existencia en los almacenes físicos (dos almacenes), retornar el diccionario con valores vacíos
        if len(inventarios_almacenes_fisicos) == 0:
            inventarios_almacenes_fisicos = [
                ('6','CDCUN01', 0),
                ('7','CDMX01', 0)
            ]

        # Convertir los inventarios a un DataFrame
        inventarios_almacenes_fisicos_df = pd.DataFrame(
            inventarios_almacenes_fisicos,
            columns=['id_almacen','whscode', 'total_existencias']  # Nombres de las columnas
        )


        # Obtener todas las ventas de inicio a fin del mes seleccionado
        if anio:
            anio_actual = anio
        else:
            anio_actual = date.today().year
    
        anio_anterior = anio_actual-1

        meses_dict = {
                'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
                'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
                'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
            }
        
        mes = mes.lower()
        mes_numero = meses_dict[mes]
        _,ultimo_dia_mes_anio_actual = calendar.monthrange(anio_actual,mes_numero)
        _,ultimo_dia_mes_anio_anterior = calendar.monthrange(anio_anterior,mes_numero)

        # Definimos las variables para el fin de mes del año actual y el anterior
        fecha_inicio_mes_anio_actual = date(anio_actual,mes_numero,1)
        fecha_fin_mes_anio_actual = date(anio_actual,mes_numero,ultimo_dia_mes_anio_actual)

        fecha_fin_mes_anio_anterior = date(anio_anterior,mes_numero,ultimo_dia_mes_anio_anterior)
        fecha_inicio_mes_anio_anterior = date(anio_anterior,mes_numero,1)
        
        # Obtener las ventas solamente del mes pero en el año actual de inicio a fin
        ventas_mes_anio_actual= self.venta_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio_mes_anio_actual, fecha_fin_mes_anio_actual)
        ventas_mes_dict_anio_actual = self.venta_service.transform_sales_gropued_by_month(ventas_mes_anio_actual)
        ventas_mes_actual_df = pd.DataFrame(ventas_mes_dict_anio_actual)


        # Obtener las ventas solamente del mes pero del año anterior
        ventas_mes_anio_anterior= self.venta_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio_mes_anio_anterior, fecha_fin_mes_anio_anterior)
        ventas_mes_dict_anio_anterior = self.venta_service.transform_sales_gropued_by_month(ventas_mes_anio_anterior)
        ventas_mes_anterior_df = pd.DataFrame(ventas_mes_dict_anio_anterior)

        #* Calculos del YTD
        # Obtener todas las ventas de inicio de año hasta el fin del mes seleccionado
        fecha_inicio_anio_actual = date(anio_actual,1,1)

        fecha_inicio_anio_anterior = date(anio_anterior,1,1)

        # Obtenemos el ytd del año anterior
        ventas_ytd_anio_anterior = self.venta_service.get_sales_grouped_by_year_and_whscode(nombre_marca,fecha_inicio_anio_anterior, fecha_fin_mes_anio_anterior)
        ventas_ytd_dict_anio_anterior = self.venta_service.transform_sales_gropued_by_year(ventas_ytd_anio_anterior)
        ventas_ytd_anio_anterior_df = pd.DataFrame(ventas_ytd_dict_anio_anterior)
        
        # Obtenemos el ytd del año actual
        ventas_ytd_anio_actual = self.venta_service.get_sales_grouped_by_year_and_whscode(nombre_marca,fecha_inicio_anio_actual, fecha_fin_mes_anio_actual)
        ventas_ytd_dict_anio_actual = self.venta_service.transform_sales_gropued_by_year(ventas_ytd_anio_actual)
        ventas_ytd_anio_actual = pd.DataFrame(ventas_ytd_dict_anio_actual)

        # Obtenemos las ventas promedio de cada tienda
        ventas_promedio_df = self.calcularVentaPromedio(nombre_marca,fecha_fin_mes_anio_actual)
        # Obtenemos los presupuestos de cada tienda
        presupuestos_tiendas = self.shared_service.get_all_store_budgets()
        # Transformamos a una lista de diccionarios los presupuestos tiendas
        presupuestos_tiendas_dict_list = [presupuesto.to_dict() for presupuesto in presupuestos_tiendas]
        presupuestos_tiendas_df = pd.DataFrame(presupuestos_tiendas_dict_list)


        reporte_cierre_mes = self.fusionar_dataframes_cierre_mes_tous(ventas_mes_anterior_df,ventas_mes_actual_df,
                                                                        ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual,
                                                                        tiendas_df,inventarios_tiendas_df, ventas_promedio_df,
                                                                        suma_stock_almacenes_virtuales, inventarios_almacenes_fisicos_df,
                                                                        presupuestos_tiendas_df)



        return reporte_cierre_mes
    

    def fusionar_dataframes_cierre_mes_tous(self, ventas_mes_anterior_df:pd.DataFrame, ventas_mes_actual_df:pd.DataFrame,
                                                ventas_ytd_anio_anterior_df:pd.DataFrame,ventas_ytd_anio_actual:pd.DataFrame,
                                                tiendas_df:pd.DataFrame, inventarios_tiendas_df:pd.DataFrame, ventas_promedio_df: pd.DataFrame,
                                                suma_stock_almacenes_virtuales: float, inventarios_almacenes_fisicos_df:pd.DataFrame,
                                                presupuestos_tiendas_df:pd.DataFrame):
        
        #Renombrar columnas de cada df
        ventas_mes_anterior_df.rename(columns={"total_venta_neta_con_iva": "venta_mensual_anio_anterior_iva"}, inplace=True)
        ventas_mes_actual_df.rename(columns={"total_venta_neta_con_iva": "venta_mensual_anio_actual_iva"}, inplace=True)
        ventas_ytd_anio_anterior_df.rename(columns={"total_venta_neta_con_iva": "ytd_anio_anterior_iva"}, inplace=True)
        ventas_ytd_anio_actual.rename(columns={"total_venta_neta_con_iva": "ytd_anio_actual_iva"}, inplace=True)

        # Eliminar columnas insecesrias
        ventas_mes_anterior_df.drop(columns=['mes'], inplace=True)
        ventas_mes_actual_df.drop(columns=['mes'], inplace=True)
        ventas_ytd_anio_anterior_df.drop(columns=['anio'], inplace=True)
        ventas_ytd_anio_actual.drop(columns=['anio'], inplace=True)
        presupuestos_tiendas_df.drop(columns=['fecha','id_presupuesto_tienda'], inplace=True)
        
        # Fusionar mes con mes
        ventas_mes_df = pd.merge(ventas_mes_anterior_df,ventas_mes_actual_df, on='whscode', how='outer')

        # Fusionar año con año
        ventas_ytd_df = pd.merge(ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual, on='whscode', how='outer')

        # Fusionar meses con el ytd
        ventas_cierre_mes_df = pd.merge(ventas_mes_df,ventas_ytd_df, on='whscode', how='outer')

        # Fusionar con presupuesto de tienda
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df, presupuestos_tiendas_df, on='whscode', how='left')


        # Calcular las variaciones
        ventas_cierre_mes_df = ventas_cierre_mes_df.fillna(0)
        
        ventas_cierre_mes_df['variacion_mes_porcentaje'] = np.where(ventas_cierre_mes_df['venta_mensual_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] / ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_mes_efectivo'] =  ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] - ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']

        ventas_cierre_mes_df['variacion_ytd_porcentaje'] = np.where(ventas_cierre_mes_df['ytd_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['ytd_anio_actual_iva'] / ventas_cierre_mes_df['ytd_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_ytd_efectivo']  =   ventas_cierre_mes_df['ytd_anio_actual_iva'] - ventas_cierre_mes_df['ytd_anio_anterior_iva']

        ventas_cierre_mes_df['variacion_vta_obj_porcentaje'] = np.where(ventas_cierre_mes_df['venta_objetivo'] != 0,
                                            (((ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] / ventas_cierre_mes_df['venta_objetivo']) - 1))*100,0)
        
        # Hacer el merge con los inventarios
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,inventarios_tiendas_df, on='whscode', how='outer')

        # Hacemos el merge con las ventas promedio
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,ventas_promedio_df, on='whscode', how='outer')

        # Calculamos el mos
        ventas_cierre_mes_df['mos'] = ventas_cierre_mes_df['existencia'] / ventas_cierre_mes_df['venta_promedio'].astype(float)

        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,tiendas_df[['whscode','nombre_sucursal','tipo_tienda','ciudad','estado_operativo','comparabilidad']],on='whscode',how='left')

        # Llenamos con 0 las existencias que no tengan coincidencia
        ventas_cierre_mes_df= ventas_cierre_mes_df.fillna(0)
        # print(ventas_cierre_mes_df.info())

        # Realizar la separacion de tijuana para no someterlo a operaciones de almacenes fisicos
        tiendas_tijuana_df = ventas_cierre_mes_df[ventas_cierre_mes_df['whscode'].str.contains('TSTJ', case=False, na=False)].copy()
        tiendas_monterrey_df = ventas_cierre_mes_df[ventas_cierre_mes_df['whscode'].str.contains('TSMTY', case=False, na=False)].copy()
        # print(tiendas_tijuana_df.info())
        # print(tiendas_tijuana_df)


        # Separamos en un df las tiendas de cancun para calcular sus porcentajes de participacion

    
        #Separamos los almacenes de cancun y cdmx
        almacen_fisico_cancun = inventarios_almacenes_fisicos_df[inventarios_almacenes_fisicos_df['whscode']=='CDCUN01'].copy() 

        almacen_fisico_cdmx = inventarios_almacenes_fisicos_df[inventarios_almacenes_fisicos_df['whscode']=='CDMX01'].copy() 


        # Filtrar filas donde 'whscode' contenga "CUN"
        tiendas_vp_cancun_df = ventas_cierre_mes_df[ventas_cierre_mes_df['whscode'].str.contains('TSCUN', case=False, na=False)].copy()

        # Filtrar filas donde 'whscode' contenga "AICM"
        tiendas_vp_cdmx_df = ventas_cierre_mes_df[ventas_cierre_mes_df['whscode'].str.contains('TSMX', case=False, na=False)].copy()

        #* Separamos en un df las tiendas de cdmx para calcular sus porcentajes de participacion
        # Realizar calculo de porcentaje de participacion en tiendas 

        porcentaje_participacion_venta_cancun = self.calcularPorcentajeDeParticipacionDeVenta(tiendas_vp_cancun_df)
        
        porcentaje_participacion_venta_cdmx = self.calcularPorcentajeDeParticipacionDeVenta(tiendas_vp_cdmx_df)
        
        # Realizar la reparticion de almacen entre las tiendas

        tiendas_cancun_con_piezas_almacen = self.calcularPiezasAlmacenFisico(porcentaje_participacion_venta_cancun, almacen_fisico_cancun)
        tiendas_cdmx_con_piezas_almacen = self.calcularPiezasAlmacenFisico(porcentaje_participacion_venta_cdmx, almacen_fisico_cdmx)

        # Realizar el calculo del mos_almacen de tiendas con almacen fisico
        tiendas_cancun_df = self.calcularMosAlmacen(tiendas_cancun_con_piezas_almacen)
        tiendas_cdmx_df = self.calcularMosAlmacen(tiendas_cdmx_con_piezas_almacen)

        #* Realizar la fuision entre los tres dfs, el de tijuana, con cancun y cdmx ya con sus almacenes de cun y cdmx
        ventas_cierre_mes_df = pd.concat([tiendas_tijuana_df, tiendas_monterrey_df, tiendas_cancun_df,tiendas_cdmx_df], ignore_index=True)
        # Llenamos con 0 los almacenes que no tengan existencia
        ventas_cierre_mes_df = ventas_cierre_mes_df.fillna(0)

        # Renombramos los campos para coincidir con el modelo de respuesta
        ventas_cierre_mes_df.rename(columns={'mos':'mos_tienda'}, inplace=True)
        ventas_cierre_mes_df.rename(columns={'existencia':'existencia_tienda'}, inplace=True)


        # print(ventas_cierre_mes_df)
        # Crear una lista de objetos EndMonthReportAGyMumuso a partir del DataFrame
        list_of_reports = [
            StoresEndMonthReportTous(**row.to_dict()) for _, row in ventas_cierre_mes_df.iterrows()
        ]

        # Formar el totales del reporte final
        total_venta_mensual_anio_anterior_iva = ventas_cierre_mes_df['venta_mensual_anio_anterior_iva'].sum()
        total_venta_mensual_anio_actual_iva = ventas_cierre_mes_df['venta_mensual_anio_actual_iva'].sum()
        if total_venta_mensual_anio_anterior_iva > 0:
            total_variacion_mes_porcentaje = ((total_venta_mensual_anio_actual_iva / total_venta_mensual_anio_anterior_iva)-1)*100
        else :
            total_variacion_mes_porcentaje = 0

        total_variacion_mes_efectivo = total_venta_mensual_anio_actual_iva - total_venta_mensual_anio_anterior_iva

        total_ytd_anio_anterior_iva = ventas_cierre_mes_df['ytd_anio_anterior_iva'].sum()
        total_ytd_anio_actual_iva = ventas_cierre_mes_df['ytd_anio_actual_iva'].sum()
        if total_ytd_anio_anterior_iva>0:
            total_variacion_ytd_porcentaje = ((total_ytd_anio_actual_iva / total_ytd_anio_anterior_iva)-1)*100
        else:
            total_variacion_ytd_porcentaje = 0
        total_variacion_ytd_efectivo = total_ytd_anio_actual_iva - total_ytd_anio_anterior_iva

        total_venta_objetivo = ventas_cierre_mes_df['venta_objetivo'].sum()
        if total_venta_objetivo == 0:
            total_variacion_vta_obj_porcentaje = 0
        else:
            total_variacion_vta_obj_porcentaje = ((total_venta_mensual_anio_actual_iva / total_venta_objetivo)-1)*100
    
        total_punto_equilibrio = ventas_cierre_mes_df['punto_equilibrio'].sum()

        total_existencia_tiendas = float(ventas_cierre_mes_df['existencia_tienda'].sum())
        total_venta_promedio = float(ventas_cierre_mes_df['venta_promedio'].sum())
        total_mos_tiendas = total_existencia_tiendas/total_venta_promedio


        # Sumamos el total de existencia de bodegas
        total_existencia_bodegas = float(ventas_cierre_mes_df['existencias_almacen'].sum())
        total_mos_bodegas = (total_existencia_tiendas + total_existencia_bodegas) / total_venta_promedio

        # Calculamos el mos de los almacenes
        total_existencia_almacenes = float(suma_stock_almacenes_virtuales)
        total_mos_almacenes = (total_existencia_tiendas + total_existencia_bodegas + total_existencia_almacenes) / total_venta_promedio

        reporte_final_cierre_mes = FinallyEndMonthReportTous(
            stores=list_of_reports,
            existencias_almacen_virtual = total_existencia_almacenes,

            total_venta_mensual_anio_anterior_iva=total_venta_mensual_anio_anterior_iva,
            total_venta_mensual_anio_actual_iva=total_venta_mensual_anio_actual_iva,
            total_variacion_mes_porcentaje=total_variacion_mes_porcentaje,
            total_variacion_mes_efectivo=total_variacion_mes_efectivo,
            total_ytd_anio_anterior_iva=total_ytd_anio_anterior_iva,
            total_ytd_anio_actual_iva=total_ytd_anio_actual_iva,
            total_variacion_ytd_porcentaje=total_variacion_ytd_porcentaje,
            total_variacion_ytd_efectivo=total_variacion_ytd_efectivo,
            total_venta_objetivo = total_venta_objetivo,
            total_variacion_vta_obj_porcentaje = total_variacion_vta_obj_porcentaje,
            total_punto_equilibrio = total_punto_equilibrio,
            total_existencia_tiendas=total_existencia_tiendas,
            total_mos_tiendas=total_mos_tiendas,
            total_existencias_bodegas=total_existencia_bodegas,
            total_mos_bodegas = total_mos_bodegas,
            total_mos_almacenes = total_mos_almacenes

        )
        
        return reporte_final_cierre_mes
    

    # Funcion para calcular el porcentaje activo de tiendas recibiendo una lista de tiendas con sus ventas promedio
    def calcularPorcentajeDeParticipacionDeVenta(self,tiendas_con_venta_promedio_df:pd.DataFrame):
        # Calculamos la suma de venta promedio de todas las tiendas
        suma_venta_promedio_tiendas = tiendas_con_venta_promedio_df['venta_promedio'].sum()

        # Calculamos el porcentaje activo de venta de cada tienda
        tiendas_con_venta_promedio_df['porcentaje_participacion_venta_tienda'] = (
            (tiendas_con_venta_promedio_df['venta_promedio'] / suma_venta_promedio_tiendas))

        return tiendas_con_venta_promedio_df
    

    # Función para calcular las piezas a repartir en base al porcentaje activo de tiendas y existencias de almacenfisico
    def calcularPiezasAlmacenFisico(self, tiendas_con_participacion_venta:pd.DataFrame, almacen_fisico_df:pd.DataFrame):
        """ Funcion que recibe de parametro un df con las tiendas con su porcentaje de participacion y las existencias de almacen
            
            Args:
                tiendas_con_participacion_venta (pd.DataFrame): Tiendas con su participacion de venta.
                almacen_fisico_df (pd.DataFrame): Almacen fisico con sus existencias.

            Returns:
                pd.DataFrame: Las tiendas con su respectiva cantidad de unidades que le corresponden del almacen.
        """
        # # Filtramos las tiendas a solo las que estan abiertas para repartir
        # tiendas_con_participacion_venta = tiendas_con_participacion_venta.copy()
        # tiendas_con_participacion_venta = tiendas_con_participacion_venta[tiendas_con_participacion_venta['estado_operativo']=='ABIERTA']
        
        if almacen_fisico_df.empty:
            tiendas_con_participacion_venta['existencias_almacen'] = 0
        else: 
            # Obtenemos el total de las existencias del almacen fisico
            total_existencias_almacen_fisico = almacen_fisico_df['total_existencias'].iloc[0]

            # Calculamos la cantidad de piezas con np.where para solo repartir entre tiendas abiertas el almacen fisico
            tiendas_con_participacion_venta['existencias_almacen'] = np.where(
                tiendas_con_participacion_venta['estado_operativo'] == 'ABIERTA',  # Condición
                total_existencias_almacen_fisico * tiendas_con_participacion_venta['porcentaje_participacion_venta_tienda'],  # Valor si es verdadera
                0  # Valor si es falsa
            )

        return tiendas_con_participacion_venta
    

    # Funcion para calcular el mos_almacen de las tiendas que cuentan con almacen
    def calcularMosAlmacen(self, tiendas_con_piezas_almacen_fisico:pd.DataFrame):
        """Funcion para calcular el mos_almacen de las tiendas con almacen fisico"""
    
        tiendas_con_piezas_almacen_fisico['mos_almacen'] = np.where(tiendas_con_piezas_almacen_fisico['venta_promedio']!=0,
            (tiendas_con_piezas_almacen_fisico['existencia'].astype(float) + tiendas_con_piezas_almacen_fisico['existencias_almacen'].astype(float))
            / tiendas_con_piezas_almacen_fisico['venta_promedio'].astype(float),0)
        
        return tiendas_con_piezas_almacen_fisico
    

    # Funcion para obtener el reporte de cierre de mes de tumi
    def get_month_end_report_tumi(self, nombre_marca:str, mes:str, anio:int, tipo_inventario:str):
        """Metodo para obtener los datos de cierre de mes de Tous Hetal Mevy y Ricodeli"""
        # print(f'mes: {mes}')
        # Obtener registros de las tiendas por marca y transformalo en df
        tiendas = [t.to_dict() for t in self.shared_service.get_all_stores_by_brand_name(nombre_marca)]
        tiendas_df = pd.DataFrame(tiendas)

        # Obtener los inventarios de todas las tiendas
        # Si el tipo de inventario es 'actual', entonces traemos los inventario de tiendas actuales, pero si es 'cierre-mes' entonces llamamos al historial del inventario del mes seleccionado
        if tipo_inventario == 'actual':
            # Si el tipo de peticion es con inventario actual obtenemos el inventario de tiendas
            inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name(nombre_marca)
            # Luego obtenemos el de los almacenes
            # Obtener la suma de stock de los almacenes virtuales
            inventarios_almacenes_virtuales = self.inventario_service.get_virtual_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
            suma_stock_almacenes_virtuales = inventarios_almacenes_virtuales[0]

            # Obtener la suma de los inventarios fisicos de los almacenes
            inventarios_almacenes_fisicos = self.inventario_service.get_physical_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
        
        elif tipo_inventario == 'cierre-mes':
            # Evaluamos que se tenga el inventario con la fecha especificada, si no se retorno nada entonces usar el cierre de mes mas cercano
            lista_historiales_inventarios = self.inventario_service.get_end_month_history_inventory_list(nombre_marca)
            # Buscamos el año y mes dentro del diccionadio de lista_historiales_inventarios
            resultado = None
            for item in lista_historiales_inventarios:
                if item['anio'] == anio:  # Buscamos el año
                    for inventario in item['inventario_cierre']:
                        if inventario['name'] == mes.lower():  # Busca el mes dentro de 'inventario_cierre'
                            resultado = inventario
                            break
                    if resultado:
                        break

            if resultado:
                # Si existe el mes entonces cargamos el inventario desde el historial de inventarios
                inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name_and_month(nombre_marca, mes, anio)
                # Si existe entonces cargamos la suma de inventarios del historial de almacenes fisicos
                # Obtener la suma de stock de los almacenes virtuales
                inventarios_almacenes_virtuales = self.inventario_service.get_history_from_virtual_warehouses_inventories_total_stock_by_brand_name(nombre_marca, mes, anio)
                suma_stock_almacenes_virtuales = int(inventarios_almacenes_virtuales)
                # Obtener la suma de los inventarios fisicos de los almacenes
                inventarios_almacenes_fisicos = self.inventario_service.get_history_from_physical_warehouses_inventories_total_stock_by_brand_name(nombre_marca, mes, anio)


            else:
                print("No se encontró el mes en el año especificado.")
                # Si no se encuentra el mes en el año especificado, entonces se obtiene el inventario actual para las tiendas
                inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name(nombre_marca)
                # Crear una nueva lista con los valores numéricos reemplazados por 0 en las existencias
                inventarios_tiendas = [(key, 0) for key, _ in inventarios_tiendas]

                # De igual manera setear en 0 la suma de existencias virtuales
                suma_stock_almacenes_virtuales = 0
                
                # Extraemos los actuales para la estuctura y los seteamos en 0
                inventarios_almacenes_fisicos = self.inventario_service.get_physical_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
                # Crear una nueva lista con el las existencias en 0
                inventarios_almacenes_fisicos = [(x, y, 0) for x, y, _ in inventarios_almacenes_fisicos]
                
        else:
            raise ValueError("El tipo de inventario no es válido")
        
        inventarios_tiendas_dict = self.inventario_service.transform_store_inventories_total_stock(inventarios_tiendas)
        inventarios_tiendas_df = pd.DataFrame(inventarios_tiendas_dict)


        # En caso de no tener existencia en los almacenes físicos (dos almacenes), retornar el diccionario con valores vacíos
        if len(inventarios_almacenes_fisicos) == 0:
            inventarios_almacenes_fisicos = [
                ('6','CDCUN01', 0),
                ('7','CDMX01', 0)
            ]

        # Convertir los inventarios a un DataFrame
        inventarios_almacenes_fisicos_df = pd.DataFrame(
            inventarios_almacenes_fisicos,
            columns=['id_almacen','whscode', 'total_existencias']  # Nombres de las columnas
        )



        # Obtener todas las ventas de inicio a fin del mes seleccionado
        if anio:
            anio_actual = anio
        else:
            anio_actual = date.today().year
    
        anio_anterior = anio_actual-1

        meses_dict = {
                'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
                'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
                'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
            }
        
        mes = mes.lower()
        mes_numero = meses_dict[mes]
        _,ultimo_dia_mes_anio_actual = calendar.monthrange(anio_actual,mes_numero)
        _,ultimo_dia_mes_anio_anterior = calendar.monthrange(anio_anterior,mes_numero)

        # Definimos las variables para el fin de mes del año actual y el anterior
        fecha_inicio_mes_anio_actual = date(anio_actual,mes_numero,1)
        fecha_fin_mes_anio_actual = date(anio_actual,mes_numero,ultimo_dia_mes_anio_actual)

        fecha_fin_mes_anio_anterior = date(anio_anterior,mes_numero,ultimo_dia_mes_anio_anterior)
        fecha_inicio_mes_anio_anterior = date(anio_anterior,mes_numero,1)
        
        # Obtener las ventas solamente del mes pero en el año actual de inicio a fin
        ventas_mes_anio_actual= self.venta_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio_mes_anio_actual, fecha_fin_mes_anio_actual)
        ventas_mes_dict_anio_actual = self.venta_service.transform_sales_gropued_by_month(ventas_mes_anio_actual)
        ventas_mes_actual_df = pd.DataFrame(ventas_mes_dict_anio_actual)


        # Obtener las ventas solamente del mes pero del año anterior
        ventas_mes_anio_anterior= self.venta_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio_mes_anio_anterior, fecha_fin_mes_anio_anterior)
        ventas_mes_dict_anio_anterior = self.venta_service.transform_sales_gropued_by_month(ventas_mes_anio_anterior)
        ventas_mes_anterior_df = pd.DataFrame(ventas_mes_dict_anio_anterior)

        #* Calculos del YTD
        # Obtener todas las ventas de inicio de año hasta el fin del mes seleccionado
        fecha_inicio_anio_actual = date(anio_actual,1,1)

        fecha_inicio_anio_anterior = date(anio_anterior,1,1)

        # Obtenemos el ytd del año anterior
        ventas_ytd_anio_anterior = self.venta_service.get_sales_grouped_by_year_and_whscode(nombre_marca,fecha_inicio_anio_anterior, fecha_fin_mes_anio_anterior)
        ventas_ytd_dict_anio_anterior = self.venta_service.transform_sales_gropued_by_year(ventas_ytd_anio_anterior)
        ventas_ytd_anio_anterior_df = pd.DataFrame(ventas_ytd_dict_anio_anterior)
        
        # Obtenemos el ytd del año actual
        ventas_ytd_anio_actual = self.venta_service.get_sales_grouped_by_year_and_whscode(nombre_marca,fecha_inicio_anio_actual, fecha_fin_mes_anio_actual)
        ventas_ytd_dict_anio_actual = self.venta_service.transform_sales_gropued_by_year(ventas_ytd_anio_actual)
        ventas_ytd_anio_actual = pd.DataFrame(ventas_ytd_dict_anio_actual)

        # Obtenemos las ventas promedio de cada tienda
        ventas_promedio_df = self.calcularVentaPromedio(nombre_marca,fecha_fin_mes_anio_actual)

        # Obtenemos los presupuestos de cada tienda
        presupuestos_tiendas = self.shared_service.get_all_store_budgets()
        # Transformamos a una lista de diccionarios los presupuestos tiendas
        presupuestos_tiendas_dict_list = [presupuesto.to_dict() for presupuesto in presupuestos_tiendas]
        presupuestos_tiendas_df = pd.DataFrame(presupuestos_tiendas_dict_list)


        reporte_cierre_mes = self.fusionar_dataframes_cierre_mes_tumi(ventas_mes_anterior_df,ventas_mes_actual_df,
                                                                        ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual,
                                                                        tiendas_df,inventarios_tiendas_df, ventas_promedio_df,
                                                                        suma_stock_almacenes_virtuales, inventarios_almacenes_fisicos_df,
                                                                        presupuestos_tiendas_df)



        return reporte_cierre_mes
    
    # Funcion para fusionar el reporte de cierre de mes de tumi
    def fusionar_dataframes_cierre_mes_tumi(self, ventas_mes_anterior_df:pd.DataFrame, ventas_mes_actual_df:pd.DataFrame,
                                                ventas_ytd_anio_anterior_df:pd.DataFrame,ventas_ytd_anio_actual:pd.DataFrame,
                                                tiendas_df:pd.DataFrame, inventarios_tiendas_df:pd.DataFrame, ventas_promedio_df: pd.DataFrame,
                                                suma_stock_almacenes_virtuales: float, inventarios_almacenes_fisicos_df:pd.DataFrame, 
                                                presupuestos_tiendas_df:pd.DataFrame):
        
        #Renombrar columnas de cada df
        ventas_mes_anterior_df.rename(columns={"total_venta_neta_con_iva": "venta_mensual_anio_anterior_iva"}, inplace=True)
        ventas_mes_actual_df.rename(columns={"total_venta_neta_con_iva": "venta_mensual_anio_actual_iva"}, inplace=True)
        ventas_ytd_anio_anterior_df.rename(columns={"total_venta_neta_con_iva": "ytd_anio_anterior_iva"}, inplace=True)
        ventas_ytd_anio_actual.rename(columns={"total_venta_neta_con_iva": "ytd_anio_actual_iva"}, inplace=True)

        # Eliminar columnas insecesrias
        ventas_mes_anterior_df.drop(columns=['mes'], inplace=True)
        ventas_mes_actual_df.drop(columns=['mes'], inplace=True)
        ventas_ytd_anio_anterior_df.drop(columns=['anio'], inplace=True)
        ventas_ytd_anio_actual.drop(columns=['anio'], inplace=True)
        presupuestos_tiendas_df.drop(columns=['fecha','id_presupuesto_tienda'], inplace=True)

        # Fusionar mes con mes
        ventas_mes_df = pd.merge(ventas_mes_anterior_df,ventas_mes_actual_df, on='whscode', how='outer')

        # Fusionar año con año
        ventas_ytd_df = pd.merge(ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual, on='whscode', how='outer')

        # Fusionar meses con el ytd
        ventas_cierre_mes_df = pd.merge(ventas_mes_df,ventas_ytd_df, on='whscode', how='outer')

        # Fusionar con presupuesto de tienda
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df, presupuestos_tiendas_df, on='whscode', how='left')


        # Calcular las variaciones
        ventas_cierre_mes_df = ventas_cierre_mes_df.fillna(0)
        
        ventas_cierre_mes_df['variacion_mes_porcentaje'] = np.where(ventas_cierre_mes_df['venta_mensual_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] / ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_mes_efectivo'] =  ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] - ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']

        ventas_cierre_mes_df['variacion_ytd_porcentaje'] = np.where(ventas_cierre_mes_df['ytd_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['ytd_anio_actual_iva'] / ventas_cierre_mes_df['ytd_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_ytd_efectivo']  =   ventas_cierre_mes_df['ytd_anio_actual_iva'] - ventas_cierre_mes_df['ytd_anio_anterior_iva']

        ventas_cierre_mes_df['variacion_vta_obj_porcentaje'] = np.where(ventas_cierre_mes_df['venta_objetivo'] != 0,
                                            (((ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] / ventas_cierre_mes_df['venta_objetivo']) - 1))*100,0)
        
        # Hacer el merge con los inventarios
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,inventarios_tiendas_df, on='whscode', how='outer')

        # Hacemos el merge con las ventas promedio
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,ventas_promedio_df, on='whscode', how='outer')

        # Calculamos el mos
        ventas_cierre_mes_df['mos'] = ventas_cierre_mes_df['existencia'] / ventas_cierre_mes_df['venta_promedio'].astype(float)

        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,tiendas_df[['whscode','nombre_sucursal','tipo_tienda','ciudad','estado_operativo','comparabilidad']],on='whscode',how='left')

        # Llenamos con 0 las existencias que no tengan coincidencia
        ventas_cierre_mes_df= ventas_cierre_mes_df.fillna(0)

        # Separar las tiendas que no son cancun ni cdmx
        tiendas_sin_cdmx_y_cun = ventas_cierre_mes_df[~ventas_cierre_mes_df['whscode'].str.contains('CUN|MEX', case=False, na=False)].copy()

        # Separamos en un df las tiendas de cancun para calcular sus porcentajes de participacion
        #Separamos los almacenes de cancun y cdmx
        almacen_fisico_cancun = inventarios_almacenes_fisicos_df[inventarios_almacenes_fisicos_df['whscode']=='CDCUN01'].copy() 

        almacen_fisico_cdmx = inventarios_almacenes_fisicos_df[inventarios_almacenes_fisicos_df['whscode']=='CDMX01'].copy() 


        # Filtrar filas donde 'whscode' contenga "CUN"
        tiendas_vp_cancun_df = ventas_cierre_mes_df[ventas_cierre_mes_df['whscode'].str.contains('CUN', case=False, na=False)].copy()
        # Filtrar filas donde 'whscode' contenga "MEX"
        tiendas_vp_cdmx_df = ventas_cierre_mes_df[ventas_cierre_mes_df['whscode'].str.contains('MEX', case=False, na=False)].copy()

        #* Separamos en un df las tiendas de cdmx para calcular sus porcentajes de participacion
        # Realizar calculo de porcentaje de participacion en tiendas 

        porcentaje_participacion_venta_cancun = self.calcularPorcentajeDeParticipacionDeVenta(tiendas_vp_cancun_df)
        
        porcentaje_participacion_venta_cdmx = self.calcularPorcentajeDeParticipacionDeVenta(tiendas_vp_cdmx_df)
        
        # Realizar la reparticion de almacen entre las tiendas

        tiendas_cancun_con_piezas_almacen = self.calcularPiezasAlmacenFisico(porcentaje_participacion_venta_cancun, almacen_fisico_cancun)
        tiendas_cdmx_con_piezas_almacen = self.calcularPiezasAlmacenFisico(porcentaje_participacion_venta_cdmx, almacen_fisico_cdmx)

        # Realizar el calculo del mos_almacen de tiendas con almacen fisico
        tiendas_cancun_df = self.calcularMosAlmacen(tiendas_cancun_con_piezas_almacen)
        tiendas_cdmx_df = self.calcularMosAlmacen(tiendas_cdmx_con_piezas_almacen)

        #* Realizar la fuision entre los tres dfs, el de tijuana, con cancun y cdmx ya con sus almacenes de cun y cdmx
        ventas_cierre_mes_df = pd.concat([tiendas_sin_cdmx_y_cun, tiendas_cancun_df,tiendas_cdmx_df], ignore_index=True)
        # Llenamos con 0 los almacenes que no tengan existencia
        ventas_cierre_mes_df = ventas_cierre_mes_df.fillna(0)

        # Renombramos los campos para coincidir con el modelo de respuesta
        ventas_cierre_mes_df.rename(columns={'mos':'mos_tienda'}, inplace=True)
        ventas_cierre_mes_df.rename(columns={'existencia':'existencia_tienda'}, inplace=True)


        # print(ventas_cierre_mes_df)
        # Crear una lista de objetos EndMonthReportAGyMumuso a partir del DataFrame
        list_of_reports = [
            StoresEndMonthReportTous(**row.to_dict()) for _, row in ventas_cierre_mes_df.iterrows()
        ]

        # Formar el totales del reporte final
        total_venta_mensual_anio_anterior_iva = ventas_cierre_mes_df['venta_mensual_anio_anterior_iva'].sum()
        total_venta_mensual_anio_actual_iva = ventas_cierre_mes_df['venta_mensual_anio_actual_iva'].sum()
        total_variacion_mes_porcentaje = ((total_venta_mensual_anio_actual_iva / total_venta_mensual_anio_anterior_iva)-1)*100
        total_variacion_mes_efectivo = total_venta_mensual_anio_actual_iva - total_venta_mensual_anio_anterior_iva

        total_ytd_anio_anterior_iva = ventas_cierre_mes_df['ytd_anio_anterior_iva'].sum()
        total_ytd_anio_actual_iva = ventas_cierre_mes_df['ytd_anio_actual_iva'].sum()
        total_variacion_ytd_porcentaje = ((total_ytd_anio_actual_iva / total_ytd_anio_anterior_iva)-1)*100
        total_variacion_ytd_efectivo = total_ytd_anio_actual_iva - total_ytd_anio_anterior_iva

        total_venta_objetivo = ventas_cierre_mes_df['venta_objetivo'].sum()
        if total_venta_objetivo == 0:
            total_variacion_vta_obj_porcentaje = 0
        else:
            total_variacion_vta_obj_porcentaje = ((total_venta_mensual_anio_actual_iva / total_venta_objetivo)-1)*100
    
        total_punto_equilibrio = ventas_cierre_mes_df['punto_equilibrio'].sum()

        total_existencia_tiendas = float(ventas_cierre_mes_df['existencia_tienda'].sum())
        total_venta_promedio = float(ventas_cierre_mes_df['venta_promedio'].sum())
        total_mos_tiendas = total_existencia_tiendas/total_venta_promedio


        # Sumamos el total de existencia de bodegas
        total_existencia_bodegas = float(ventas_cierre_mes_df['existencias_almacen'].sum())
        total_mos_bodegas = (total_existencia_tiendas + total_existencia_bodegas) / total_venta_promedio

        # Calculamos el mos de los almacenes
        total_existencia_almacenes = float(suma_stock_almacenes_virtuales)
        total_mos_almacenes = (total_existencia_tiendas + total_existencia_bodegas + total_existencia_almacenes) / total_venta_promedio

        reporte_final_cierre_mes = FinallyEndMonthReportTous(
            stores=list_of_reports,
            existencias_almacen_virtual = total_existencia_almacenes,

            total_venta_mensual_anio_anterior_iva=total_venta_mensual_anio_anterior_iva,
            total_venta_mensual_anio_actual_iva=total_venta_mensual_anio_actual_iva,
            total_variacion_mes_porcentaje=total_variacion_mes_porcentaje,
            total_variacion_mes_efectivo=total_variacion_mes_efectivo,
            total_ytd_anio_anterior_iva=total_ytd_anio_anterior_iva,
            total_ytd_anio_actual_iva=total_ytd_anio_actual_iva,
            total_variacion_ytd_porcentaje=total_variacion_ytd_porcentaje,
            total_variacion_ytd_efectivo=total_variacion_ytd_efectivo,
            total_venta_objetivo = total_venta_objetivo,
            total_variacion_vta_obj_porcentaje = total_variacion_vta_obj_porcentaje,
            total_punto_equilibrio = total_punto_equilibrio,

            total_existencia_tiendas=total_existencia_tiendas,
            total_mos_tiendas=total_mos_tiendas,

            total_existencias_bodegas=total_existencia_bodegas,
            total_mos_bodegas = total_mos_bodegas,
            total_mos_almacenes = total_mos_almacenes

        )
        
        return reporte_final_cierre_mes
    

    # Funcion para obtener el reporte de cierre de mes de 
    def get_month_end_report_unode50(self, nombre_marca:str, mes:str, anio:int, tipo_inventario:str):
        """Metodo para obtener los datos de cierre de mes de Tous Hetal Mevy y Ricodeli"""
        # print(f'mes: {mes}')
        # Obtener registros de las tiendas por marca y transformalo en df
        tiendas = [t.to_dict() for t in self.shared_service.get_all_stores_by_brand_name(nombre_marca)]
        tiendas_df = pd.DataFrame(tiendas)

        # Obtener los inventarios de todas las tiendas
        # Si el tipo de inventario es 'actual', entonces traemos los inventario de tiendas actuales, pero si es 'cierre-mes' entonces llamamos al historial del inventario del mes seleccionado
        if tipo_inventario == 'actual':
            # Si el tipo de peticion es con inventario actual obtenemos el inventario de tiendas
            inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name(nombre_marca)
            # Luego obtenemos el de los almacenes
            # Obtener la suma de stock de los almacenes virtuales
            inventarios_almacenes_virtuales = self.inventario_service.get_virtual_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
            suma_stock_almacenes_virtuales = inventarios_almacenes_virtuales[0]

            # Obtener la suma de los inventarios fisicos de los almacenes
            inventarios_almacenes_fisicos = self.inventario_service.get_physical_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
        
        elif tipo_inventario == 'cierre-mes':
            # Evaluamos que se tenga el inventario con la fecha especificada, si no se retorno nada entonces usar el cierre de mes mas cercano
            lista_historiales_inventarios = self.inventario_service.get_end_month_history_inventory_list(nombre_marca)
            # Buscamos el año y mes dentro del diccionadio de lista_historiales_inventarios
            resultado = None
            for item in lista_historiales_inventarios:
                if item['anio'] == anio:  # Buscamos el año
                    for inventario in item['inventario_cierre']:
                        if inventario['name'] == mes.lower():  # Busca el mes dentro de 'inventario_cierre'
                            resultado = inventario
                            break
                    if resultado:
                        break

            if resultado:
                # Si existe el mes entonces cargamos el inventario desde el historial de inventarios
                inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name_and_month(nombre_marca, mes, anio)
                # Si existe entonces cargamos la suma de inventarios del historial de almacenes fisicos
                # Obtener la suma de stock de los almacenes virtuales
                inventarios_almacenes_virtuales = self.inventario_service.get_history_from_virtual_warehouses_inventories_total_stock_by_brand_name(nombre_marca, mes, anio)
                suma_stock_almacenes_virtuales = int(inventarios_almacenes_virtuales)
                # Obtener la suma de los inventarios fisicos de los almacenes
                inventarios_almacenes_fisicos = self.inventario_service.get_history_from_physical_warehouses_inventories_total_stock_by_brand_name(nombre_marca, mes, anio)


            else:
                print("No se encontró el mes en el año especificado.")
                # Si no se encuentra el mes en el año especificado, entonces se obtiene el inventario actual para las tiendas
                inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name(nombre_marca)
                # Crear una nueva lista con los valores numéricos reemplazados por 0 en las existencias
                inventarios_tiendas = [(key, 0) for key, _ in inventarios_tiendas]

                # De igual manera setear en 0 la suma de existencias virtuales
                suma_stock_almacenes_virtuales = 0
                
                # Extraemos los actuales para la estuctura y los seteamos en 0
                inventarios_almacenes_fisicos = self.inventario_service.get_physical_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
                # Crear una nueva lista con el las existencias en 0
                inventarios_almacenes_fisicos = [(x, y, 0) for x, y, _ in inventarios_almacenes_fisicos]
                
        else:
            raise ValueError("El tipo de inventario no es válido")
        
        inventarios_tiendas_dict = self.inventario_service.transform_store_inventories_total_stock(inventarios_tiendas)
        inventarios_tiendas_df = pd.DataFrame(inventarios_tiendas_dict)
        
        # En caso de no tener existencia en los almacenes físicos (dos almacenes), retornar el diccionario con valores vacíos
        if len(inventarios_almacenes_fisicos) == 0:
            inventarios_almacenes_fisicos = [
                ('7','CDMX01', 0)
            ]

        # Convertir los inventarios a un DataFrame
        inventarios_almacenes_fisicos_df = pd.DataFrame(
            inventarios_almacenes_fisicos,
            columns=['id_almacen','whscode', 'total_existencias']  # Nombres de las columnas
        )



        # Obtener todas las ventas de inicio a fin del mes seleccionado
        if anio:
            anio_actual = anio
        else:
            anio_actual = date.today().year
    
        anio_anterior = anio_actual-1

        meses_dict = {
                'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
                'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
                'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
            }
        
        mes = mes.lower()
        mes_numero = meses_dict[mes]
        _,ultimo_dia_mes_anio_actual = calendar.monthrange(anio_actual,mes_numero)
        _,ultimo_dia_mes_anio_anterior = calendar.monthrange(anio_anterior,mes_numero)

        # Definimos las variables para el fin de mes del año actual y el anterior
        fecha_inicio_mes_anio_actual = date(anio_actual,mes_numero,1)
        fecha_fin_mes_anio_actual = date(anio_actual,mes_numero,ultimo_dia_mes_anio_actual)

        fecha_fin_mes_anio_anterior = date(anio_anterior,mes_numero,ultimo_dia_mes_anio_anterior)
        fecha_inicio_mes_anio_anterior = date(anio_anterior,mes_numero,1)
        
        # Obtener las ventas solamente del mes pero en el año actual de inicio a fin
        ventas_mes_anio_actual= self.venta_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio_mes_anio_actual, fecha_fin_mes_anio_actual)
        ventas_mes_dict_anio_actual = self.venta_service.transform_sales_gropued_by_month(ventas_mes_anio_actual)
        ventas_mes_actual_df = pd.DataFrame(ventas_mes_dict_anio_actual)


        # Obtener las ventas solamente del mes pero del año anterior
        ventas_mes_anio_anterior= self.venta_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio_mes_anio_anterior, fecha_fin_mes_anio_anterior)
        ventas_mes_dict_anio_anterior = self.venta_service.transform_sales_gropued_by_month(ventas_mes_anio_anterior)
        ventas_mes_anterior_df = pd.DataFrame(ventas_mes_dict_anio_anterior)

        #* Calculos del YTD
        # Obtener todas las ventas de inicio de año hasta el fin del mes seleccionado
        fecha_inicio_anio_actual = date(anio_actual,1,1)

        fecha_inicio_anio_anterior = date(anio_anterior,1,1)

        # Obtenemos el ytd del año anterior
        ventas_ytd_anio_anterior = self.venta_service.get_sales_grouped_by_year_and_whscode(nombre_marca,fecha_inicio_anio_anterior, fecha_fin_mes_anio_anterior)
        ventas_ytd_dict_anio_anterior = self.venta_service.transform_sales_gropued_by_year(ventas_ytd_anio_anterior)
        ventas_ytd_anio_anterior_df = pd.DataFrame(ventas_ytd_dict_anio_anterior)
        
        # Obtenemos el ytd del año actual
        ventas_ytd_anio_actual = self.venta_service.get_sales_grouped_by_year_and_whscode(nombre_marca,fecha_inicio_anio_actual, fecha_fin_mes_anio_actual)
        ventas_ytd_dict_anio_actual = self.venta_service.transform_sales_gropued_by_year(ventas_ytd_anio_actual)
        ventas_ytd_anio_actual = pd.DataFrame(ventas_ytd_dict_anio_actual)

        # Obtenemos las ventas promedio de cada tienda
        ventas_promedio_df = self.calcularVentaPromedio(nombre_marca,fecha_fin_mes_anio_actual)

        # Obtenemos los presupuestos de cada tienda
        presupuestos_tiendas = self.shared_service.get_all_store_budgets()
        # Transformamos a una lista de diccionarios los presupuestos tiendas
        presupuestos_tiendas_dict_list = [presupuesto.to_dict() for presupuesto in presupuestos_tiendas]
        presupuestos_tiendas_df = pd.DataFrame(presupuestos_tiendas_dict_list)


        reporte_cierre_mes = self.fusionar_dataframes_cierre_mes_unode50(ventas_mes_anterior_df,ventas_mes_actual_df,
                                                                        ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual,
                                                                        tiendas_df,inventarios_tiendas_df, ventas_promedio_df,
                                                                        suma_stock_almacenes_virtuales, inventarios_almacenes_fisicos_df, presupuestos_tiendas_df)



        return reporte_cierre_mes
    

    # Funcion para fusionar el reporte de cierre de mes de tumi
    def fusionar_dataframes_cierre_mes_unode50(self, ventas_mes_anterior_df:pd.DataFrame, ventas_mes_actual_df:pd.DataFrame,
                                                ventas_ytd_anio_anterior_df:pd.DataFrame,ventas_ytd_anio_actual:pd.DataFrame,
                                                tiendas_df:pd.DataFrame, inventarios_tiendas_df:pd.DataFrame, ventas_promedio_df: pd.DataFrame,
                                                suma_stock_almacenes_virtuales: float, inventarios_almacenes_fisicos_df:pd.DataFrame,
                                                presupuestos_tiendas_df:pd.DataFrame ):
        
        #Renombrar columnas de cada df
        ventas_mes_anterior_df.rename(columns={"total_venta_neta_con_iva": "venta_mensual_anio_anterior_iva"}, inplace=True)
        ventas_mes_actual_df.rename(columns={"total_venta_neta_con_iva": "venta_mensual_anio_actual_iva"}, inplace=True)
        ventas_ytd_anio_anterior_df.rename(columns={"total_venta_neta_con_iva": "ytd_anio_anterior_iva"}, inplace=True)
        ventas_ytd_anio_actual.rename(columns={"total_venta_neta_con_iva": "ytd_anio_actual_iva"}, inplace=True)

        # Eliminar columnas insecesrias
        ventas_mes_anterior_df.drop(columns=['mes'], inplace=True)
        ventas_mes_actual_df.drop(columns=['mes'], inplace=True)
        ventas_ytd_anio_anterior_df.drop(columns=['anio'], inplace=True)
        ventas_ytd_anio_actual.drop(columns=['anio'], inplace=True)
        presupuestos_tiendas_df.drop(columns=['fecha','id_presupuesto_tienda'], inplace=True)

        # Fusionar mes con mes
        ventas_mes_df = pd.merge(ventas_mes_anterior_df,ventas_mes_actual_df, on='whscode', how='outer')

        # Fusionar año con año
        ventas_ytd_df = pd.merge(ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual, on='whscode', how='outer')

        # Fusionar meses con el ytd
        ventas_cierre_mes_df = pd.merge(ventas_mes_df,ventas_ytd_df, on='whscode', how='outer')

        # Fusionar con presupuesto de tienda
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df, presupuestos_tiendas_df, on='whscode', how='left')


        # Calcular las variaciones
        ventas_cierre_mes_df = ventas_cierre_mes_df.fillna(0)
        
        ventas_cierre_mes_df['variacion_mes_porcentaje'] = np.where(ventas_cierre_mes_df['venta_mensual_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] / ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_mes_efectivo'] =  ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] - ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']

        ventas_cierre_mes_df['variacion_ytd_porcentaje'] = np.where(ventas_cierre_mes_df['ytd_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['ytd_anio_actual_iva'] / ventas_cierre_mes_df['ytd_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_ytd_efectivo']  =   ventas_cierre_mes_df['ytd_anio_actual_iva'] - ventas_cierre_mes_df['ytd_anio_anterior_iva']

        ventas_cierre_mes_df['variacion_vta_obj_porcentaje'] = np.where(ventas_cierre_mes_df['venta_objetivo'] != 0,
                                            (((ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] / ventas_cierre_mes_df['venta_objetivo']) - 1))*100,0)
        
        # Hacer el merge con los inventarios
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,inventarios_tiendas_df, on='whscode', how='outer')

        # Hacemos el merge con las ventas promedio
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,ventas_promedio_df, on='whscode', how='outer')

        # Calculamos el mos
        ventas_cierre_mes_df['mos'] = ventas_cierre_mes_df['existencia'] / ventas_cierre_mes_df['venta_promedio'].astype(float)

        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,tiendas_df[['whscode','nombre_sucursal','tipo_tienda','ciudad','estado_operativo','comparabilidad']],on='whscode',how='left')

        # Llenamos con 0 las existencias que no tengan coincidencia
        ventas_cierre_mes_df= ventas_cierre_mes_df.fillna(0)


        # Separar las tiendas que no son cancun ni cdmx
        tiendas_sin_cdmx = ventas_cierre_mes_df[~ventas_cierre_mes_df['whscode'].str.contains('MX', case=False, na=False)].copy()

        # Separamos en un df las tiendas de cancun para calcular sus porcentajes de participacion

        almacen_fisico_cdmx = inventarios_almacenes_fisicos_df[inventarios_almacenes_fisicos_df['whscode']=='CDMX01'].copy() 

        # Filtrar filas donde 'whscode' contenga "MEX"
        tiendas_vp_cdmx_df = ventas_cierre_mes_df[ventas_cierre_mes_df['whscode'].str.contains('MX', case=False, na=False)].copy()

        #* Separamos en un df las tiendas de cdmx para calcular sus porcentajes de participacion
        # Realizar calculo de porcentaje de participacion en tiendas 

        porcentaje_participacion_venta_cdmx = self.calcularPorcentajeDeParticipacionDeVenta(tiendas_vp_cdmx_df)
        
        # Realizar la reparticion de almacen entre las tiendas

        tiendas_cdmx_con_piezas_almacen = self.calcularPiezasAlmacenFisico(porcentaje_participacion_venta_cdmx, almacen_fisico_cdmx)

        # Realizar el calculo del mos_almacen de tiendas con almacen fisico
        tiendas_cdmx_df = self.calcularMosAlmacen(tiendas_cdmx_con_piezas_almacen)

        #* Realizar la fuision entre los tres dfs, el de tijuana, con cancun y cdmx ya con sus almacenes de cun y cdmx
        ventas_cierre_mes_df = pd.concat([tiendas_sin_cdmx,tiendas_cdmx_df], ignore_index=True)
        # Llenamos con 0 los almacenes que no tengan existencia
        ventas_cierre_mes_df = ventas_cierre_mes_df.fillna(0)

        # Renombramos los campos para coincidir con el modelo de respuesta
        ventas_cierre_mes_df.rename(columns={'mos':'mos_tienda'}, inplace=True)
        ventas_cierre_mes_df.rename(columns={'existencia':'existencia_tienda'}, inplace=True)


        # print(ventas_cierre_mes_df)
        # Crear una lista de objetos EndMonthReportAGyMumuso a partir del DataFrame
        list_of_reports = [
            StoresEndMonthReportTous(**row.to_dict()) for _, row in ventas_cierre_mes_df.iterrows()
        ]

        # Formar el totales del reporte final
        total_venta_mensual_anio_anterior_iva = ventas_cierre_mes_df['venta_mensual_anio_anterior_iva'].sum()
        total_venta_mensual_anio_actual_iva = ventas_cierre_mes_df['venta_mensual_anio_actual_iva'].sum()
        total_variacion_mes_porcentaje = ((total_venta_mensual_anio_actual_iva / total_venta_mensual_anio_anterior_iva)-1)*100
        total_variacion_mes_efectivo = total_venta_mensual_anio_actual_iva - total_venta_mensual_anio_anterior_iva

        total_ytd_anio_anterior_iva = ventas_cierre_mes_df['ytd_anio_anterior_iva'].sum()
        total_ytd_anio_actual_iva = ventas_cierre_mes_df['ytd_anio_actual_iva'].sum()
        total_variacion_ytd_porcentaje = ((total_ytd_anio_actual_iva / total_ytd_anio_anterior_iva)-1)*100
        total_variacion_ytd_efectivo = total_ytd_anio_actual_iva - total_ytd_anio_anterior_iva

        total_venta_objetivo = ventas_cierre_mes_df['venta_objetivo'].sum()
        if total_venta_objetivo == 0:
            total_variacion_vta_obj_porcentaje = 0
        else:
            total_variacion_vta_obj_porcentaje = ((total_venta_mensual_anio_actual_iva / total_venta_objetivo)-1)*100
    
        total_punto_equilibrio = ventas_cierre_mes_df['punto_equilibrio'].sum()


        total_existencia_tiendas = float(ventas_cierre_mes_df['existencia_tienda'].sum())
        total_venta_promedio = float(ventas_cierre_mes_df['venta_promedio'].sum())
        total_mos_tiendas = total_existencia_tiendas/total_venta_promedio


        # Sumamos el total de existencia de bodegas
        total_existencia_bodegas = float(ventas_cierre_mes_df['existencias_almacen'].sum())
        total_mos_bodegas = (total_existencia_tiendas + total_existencia_bodegas) / total_venta_promedio

        # Calculamos el mos de los almacenes
        total_existencia_almacenes = float(suma_stock_almacenes_virtuales)
        total_mos_almacenes = (total_existencia_tiendas + total_existencia_bodegas + total_existencia_almacenes) / total_venta_promedio

        reporte_final_cierre_mes = FinallyEndMonthReportTous(
            stores=list_of_reports,
            existencias_almacen_virtual = total_existencia_almacenes,

            total_venta_mensual_anio_anterior_iva=total_venta_mensual_anio_anterior_iva,
            total_venta_mensual_anio_actual_iva=total_venta_mensual_anio_actual_iva,
            total_variacion_mes_porcentaje=total_variacion_mes_porcentaje,
            total_variacion_mes_efectivo=total_variacion_mes_efectivo,
            total_ytd_anio_anterior_iva=total_ytd_anio_anterior_iva,
            total_ytd_anio_actual_iva=total_ytd_anio_actual_iva,
            total_variacion_ytd_porcentaje=total_variacion_ytd_porcentaje,
            total_variacion_ytd_efectivo=total_variacion_ytd_efectivo,

            total_venta_objetivo = total_venta_objetivo,
            total_variacion_vta_obj_porcentaje = total_variacion_vta_obj_porcentaje,
            total_punto_equilibrio = total_punto_equilibrio,

            total_existencia_tiendas=total_existencia_tiendas,
            total_mos_tiendas=total_mos_tiendas,

            total_mos_bodegas = total_mos_bodegas,
            total_mos_almacenes = total_mos_almacenes

        )
        
        return reporte_final_cierre_mes
    
    
    # Funcion para obtener el reporte de cierre de mes de 
    def get_month_end_report_penguin(self, nombre_marca:str, mes:str, anio:int, tipo_inventario:str):
        """Metodo para obtener los datos de cierre de mes de Penguin"""
        # print(f'mes: {mes}')
        # Obtener registros de las tiendas por marca y transformalo en df
        tiendas = [t.to_dict() for t in self.shared_service.get_all_stores_by_brand_name(nombre_marca)]
        tiendas_df = pd.DataFrame(tiendas)
        tiendas_df['whscode'] = tiendas_df['whscode'].replace({'PENGDLCO': 'PENGDL01'})

        # Fusionamos las tiendas en una sola fila
        tiendas_df = tiendas_df.groupby('whscode').first().reset_index()


        # Obtener los inventarios de todas las tiendas
        # Si el tipo de inventario es 'actual', entonces traemos los inventario de tiendas actuales, pero si es 'cierre-mes' entonces llamamos al historial del inventario del mes seleccionado
        if tipo_inventario == 'actual':
            # Si el tipo de peticion es con inventario actual obtenemos el inventario de tiendas
            inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name(nombre_marca)
            # Luego obtenemos el de los almacenes
            # Obtener la suma de stock de los almacenes virtuales
            inventarios_almacenes_virtuales = self.inventario_service.get_virtual_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
            suma_stock_almacenes_virtuales = inventarios_almacenes_virtuales[0]

        elif tipo_inventario == 'cierre-mes':
            # Evaluamos que se tenga el inventario con la fecha especificada, si no se retorno nada entonces usar el cierre de mes mas cercano
            lista_historiales_inventarios = self.inventario_service.get_end_month_history_inventory_list(nombre_marca)
            # Buscamos el año y mes dentro del diccionadio de lista_historiales_inventarios
            resultado = None
            for item in lista_historiales_inventarios:
                if item['anio'] == anio:  # Buscamos el año
                    for inventario in item['inventario_cierre']:
                        if inventario['name'] == mes.lower():  # Busca el mes dentro de 'inventario_cierre'
                            resultado = inventario
                            break
                    if resultado:
                        break

            if resultado:
                # Si existe el mes entonces cargamos el inventario desde el historial de inventarios
                inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name_and_month(nombre_marca, mes, anio)
                # Si existe entonces cargamos la suma de inventarios del historial de almacenes fisicos
                # Obtener la suma de stock de los almacenes virtuales
                inventarios_almacenes_virtuales = self.inventario_service.get_history_from_virtual_warehouses_inventories_total_stock_by_brand_name(nombre_marca, mes, anio)
                suma_stock_almacenes_virtuales = int(inventarios_almacenes_virtuales)

            else:
                print("No se encontró el mes en el año especificado.")
                # Si no se encuentra el mes en el año especificado, entonces se obtiene el inventario actual para las tiendas
                inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name(nombre_marca)
                # Crear una nueva lista con los valores numéricos reemplazados por 0 en las existencias
                inventarios_tiendas = [(key, 0) for key, _ in inventarios_tiendas]

                # De igual manera setear en 0 la suma de existencias virtuales
                suma_stock_almacenes_virtuales = 0
                
        else:
            raise ValueError("El tipo de inventario no es válido")
        
        inventarios_tiendas_dict = self.inventario_service.transform_store_inventories_total_stock(inventarios_tiendas)
        inventarios_tiendas_df = pd.DataFrame(inventarios_tiendas_dict)
        # Fusionamos los dos penguin
        inventarios_tiendas_df['whscode'] = inventarios_tiendas_df['whscode'].replace({'PENGDLCO': 'PENGDL01'})
        # Agrupamos por whscode
        inventarios_tiendas_df = inventarios_tiendas_df.groupby('whscode').agg({'existencia':'sum'}).reset_index()


        # Obtener todas las ventas de inicio a fin del mes seleccionado
        if anio:
            anio_actual = anio
        else:
            anio_actual = date.today().year
    
        anio_anterior = anio_actual-1

        meses_dict = {
                'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
                'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
                'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
            }
        
        mes = mes.lower()
        mes_numero = meses_dict[mes]
        _,ultimo_dia_mes_anio_actual = calendar.monthrange(anio_actual,mes_numero)
        _,ultimo_dia_mes_anio_anterior = calendar.monthrange(anio_anterior,mes_numero)

        # Definimos las variables para el fin de mes del año actual y el anterior
        fecha_inicio_mes_anio_actual = date(anio_actual,mes_numero,1)
        fecha_fin_mes_anio_actual = date(anio_actual,mes_numero,ultimo_dia_mes_anio_actual)

        fecha_fin_mes_anio_anterior = date(anio_anterior,mes_numero,ultimo_dia_mes_anio_anterior)
        fecha_inicio_mes_anio_anterior = date(anio_anterior,mes_numero,1)
        
        # Obtener las ventas solamente del mes pero en el año actual de inicio a fin
        ventas_mes_anio_actual= self.venta_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio_mes_anio_actual, fecha_fin_mes_anio_actual)
        ventas_mes_dict_anio_actual = self.venta_service.transform_sales_gropued_by_month(ventas_mes_anio_actual)
        ventas_mes_actual_df = pd.DataFrame(ventas_mes_dict_anio_actual)
        
        # Fusionamos los dos penguin
        ventas_mes_actual_df['whscode'] = ventas_mes_actual_df['whscode'].replace({'PENGDLCO': 'PENGDL01'})
        # Agrupamos por whscode
        ventas_mes_actual_df = ventas_mes_actual_df.groupby('whscode').agg({'mes':'first','total_venta_neta_con_iva':'sum'}).reset_index()



        # Obtener las ventas solamente del mes pero del año anterior
        ventas_mes_anio_anterior= self.venta_service.get_sales_grouped_by_month_and_whscode(nombre_marca, fecha_inicio_mes_anio_anterior, fecha_fin_mes_anio_anterior)
        ventas_mes_dict_anio_anterior = self.venta_service.transform_sales_gropued_by_month(ventas_mes_anio_anterior)
        ventas_mes_anterior_df = pd.DataFrame(ventas_mes_dict_anio_anterior, columns=['mes','whscode','total_venta_neta_con_iva'])
        
        if ventas_mes_anterior_df.empty:
            # Diccionario con los datos de la fila
            fila_vacia = {'mes':fecha_inicio_mes_anio_anterior, 'whscode':'PENGDL01', 'total_venta_neta_con_iva': 0}
            
            # Agregar la fila al DataFrame
            ventas_mes_anterior_df = pd.concat([ventas_mes_anterior_df, pd.DataFrame([fila_vacia])], ignore_index=True)

        # Fusionamos los dos penguin
        ventas_mes_anterior_df['whscode'] = ventas_mes_anterior_df['whscode'].replace({'PENGDLCO': 'PENGDL01'})
        # Agrupamos por whscode
        ventas_mes_anterior_df = ventas_mes_anterior_df.groupby('whscode').agg({'mes':'first','total_venta_neta_con_iva':'sum'}).reset_index()

        #* Calculos del YTD
        # Obtener todas las ventas de inicio de año hasta el fin del mes seleccionado
        fecha_inicio_anio_actual = date(anio_actual,1,1)

        fecha_inicio_anio_anterior = date(anio_anterior,1,1)

        # Obtenemos el ytd del año anterior
        ventas_ytd_anio_anterior = self.venta_service.get_sales_grouped_by_year_and_whscode(nombre_marca,fecha_inicio_anio_anterior, fecha_fin_mes_anio_anterior)
        ventas_ytd_dict_anio_anterior = self.venta_service.transform_sales_gropued_by_year(ventas_ytd_anio_anterior)
        ventas_ytd_anio_anterior_df = pd.DataFrame(ventas_ytd_dict_anio_anterior)

        if ventas_ytd_anio_anterior_df.empty:
            # Diccionario con los datos de la fila
            fila_vacia = {'anio':fecha_inicio_anio_anterior, 'whscode':'PENGDL01', 'total_venta_neta_con_iva': 0}
            
            # Agregar la fila al DataFrame
            ventas_ytd_anio_anterior_df = pd.concat([ventas_ytd_anio_anterior_df, pd.DataFrame([fila_vacia])], ignore_index=True)
        
        # Fusionamos los dos penguin
        ventas_ytd_anio_anterior_df['whscode'] = ventas_ytd_anio_anterior_df['whscode'].replace({'PENGDLCO': 'PENGDL01'})
        # Agrupamos por whscode
        ventas_ytd_anio_anterior_df = ventas_ytd_anio_anterior_df.groupby('whscode').agg({'anio':'first','total_venta_neta_con_iva':'sum'}).reset_index()

        
        # Obtenemos el ytd del año actual
        ventas_ytd_anio_actual = self.venta_service.get_sales_grouped_by_year_and_whscode(nombre_marca,fecha_inicio_anio_actual, fecha_fin_mes_anio_actual)
        ventas_ytd_dict_anio_actual = self.venta_service.transform_sales_gropued_by_year(ventas_ytd_anio_actual)
        ventas_ytd_anio_actual = pd.DataFrame(ventas_ytd_dict_anio_actual)

        # Fusionamos los dos penguin
        ventas_ytd_anio_actual['whscode'] = ventas_ytd_anio_actual['whscode'].replace({'PENGDLCO': 'PENGDL01'})
        # Agrupamos por whscode
        ventas_ytd_anio_actual = ventas_ytd_anio_actual.groupby('whscode').agg({'anio':'first','total_venta_neta_con_iva':'sum'}).reset_index()


        # Obtenemos las ventas promedio de cada tienda
        ventas_promedio_df = self.calcularVentaPromedio(nombre_marca,fecha_fin_mes_anio_actual)
        # Fusionamos los dos penguin
        ventas_promedio_df['whscode'] = ventas_promedio_df['whscode'].replace({'PENGDLCO': 'PENGDL01'})
        # Agrupamos por whscode
        ventas_promedio_df = ventas_promedio_df.groupby('whscode').agg({'meses_con_venta':'first','total_ventas':'sum'}).reset_index()

        # Calculamos la venta promedio con los datos
        ventas_promedio_df['venta_promedio'] = ventas_promedio_df['total_ventas']/ventas_promedio_df['meses_con_venta']

        # Obtenemos los presupuestos de cada tienda
        presupuestos_tiendas = self.shared_service.get_all_store_budgets()
        # Transformamos a una lista de diccionarios los presupuestos tiendas
        presupuestos_tiendas_dict_list = [presupuesto.to_dict() for presupuesto in presupuestos_tiendas]
        presupuestos_tiendas_df = pd.DataFrame(presupuestos_tiendas_dict_list)

        reporte_cierre_mes = self.fusionar_dataframes_cierre_mes_penguin(ventas_mes_anterior_df,ventas_mes_actual_df,
                                                                        ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual,
                                                                        tiendas_df,inventarios_tiendas_df, ventas_promedio_df,
                                                                        suma_stock_almacenes_virtuales, presupuestos_tiendas_df)



        return reporte_cierre_mes
    

    # Funcion para fusionar el reporte de cierre de mes de tumi
    def fusionar_dataframes_cierre_mes_penguin(self, ventas_mes_anterior_df:pd.DataFrame, ventas_mes_actual_df:pd.DataFrame,
                                                ventas_ytd_anio_anterior_df:pd.DataFrame,ventas_ytd_anio_actual:pd.DataFrame,
                                                tiendas_df:pd.DataFrame, inventarios_tiendas_df:pd.DataFrame, ventas_promedio_df: pd.DataFrame,
                                                suma_stock_almacenes_virtuales: float, presupuestos_tiendas_df:pd.DataFrame ):
        
        #Renombrar columnas de cada df
        ventas_mes_anterior_df.rename(columns={"total_venta_neta_con_iva": "venta_mensual_anio_anterior_iva"}, inplace=True)
        ventas_mes_actual_df.rename(columns={"total_venta_neta_con_iva": "venta_mensual_anio_actual_iva"}, inplace=True)
        ventas_ytd_anio_anterior_df.rename(columns={"total_venta_neta_con_iva": "ytd_anio_anterior_iva"}, inplace=True)
        ventas_ytd_anio_actual.rename(columns={"total_venta_neta_con_iva": "ytd_anio_actual_iva"}, inplace=True)

        # Eliminar columnas insecesrias
        ventas_mes_anterior_df.drop(columns=['mes'], inplace=True)
        ventas_mes_actual_df.drop(columns=['mes'], inplace=True)
        ventas_ytd_anio_anterior_df.drop(columns=['anio'], inplace=True)
        ventas_ytd_anio_actual.drop(columns=['anio'], inplace=True)
        presupuestos_tiendas_df.drop(columns=['fecha','id_presupuesto_tienda'], inplace=True)
        
        # Fusionar mes con mes
        ventas_mes_df = pd.merge(ventas_mes_anterior_df,ventas_mes_actual_df, on='whscode', how='outer')

        # Fusionar año con año
        ventas_ytd_df = pd.merge(ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual, on='whscode', how='outer')

        # Fusionar meses con el ytd
        ventas_cierre_mes_df = pd.merge(ventas_mes_df,ventas_ytd_df, on='whscode', how='outer')

        # Fusionar con presupuesto de tienda
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df, presupuestos_tiendas_df, on='whscode', how='left')


        # Calcular las variaciones
        ventas_cierre_mes_df = ventas_cierre_mes_df.fillna(0)
        
        ventas_cierre_mes_df['variacion_mes_porcentaje'] = np.where(ventas_cierre_mes_df['venta_mensual_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] / ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_mes_efectivo'] =  ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] - ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']

        ventas_cierre_mes_df['variacion_ytd_porcentaje'] = np.where(ventas_cierre_mes_df['ytd_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['ytd_anio_actual_iva'] / ventas_cierre_mes_df['ytd_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_ytd_efectivo']  =   ventas_cierre_mes_df['ytd_anio_actual_iva'] - ventas_cierre_mes_df['ytd_anio_anterior_iva']

        ventas_cierre_mes_df['variacion_vta_obj_porcentaje'] = np.where(ventas_cierre_mes_df['venta_objetivo'] != 0,
                                            (((ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] / ventas_cierre_mes_df['venta_objetivo']) - 1))*100,0)
        

        # Hacer el merge con los inventarios
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,inventarios_tiendas_df, on='whscode', how='outer')

        # Hacemos el merge con las ventas promedio
        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,ventas_promedio_df, on='whscode', how='outer')

        # Calculamos el mos
        ventas_cierre_mes_df['mos'] = ventas_cierre_mes_df['existencia'] / ventas_cierre_mes_df['venta_promedio'].astype(float)

        ventas_cierre_mes_df = pd.merge(ventas_cierre_mes_df,tiendas_df[['whscode','nombre_sucursal','tipo_tienda','ciudad','estado_operativo','comparabilidad']],on='whscode',how='left')

        # Llenamos con 0 las existencias que no tengan coincidencia
        ventas_cierre_mes_df= ventas_cierre_mes_df.fillna(0)



        #* Separamos en un df las tiendas de cdmx para calcular sus porcentajes de participacion
        # Realizar calculo de porcentaje de participacion en tiendas 


        # Renombramos los campos para coincidir con el modelo de respuesta
        ventas_cierre_mes_df.rename(columns={'mos':'mos_tienda'}, inplace=True)
        ventas_cierre_mes_df.rename(columns={'existencia':'existencia_tienda'}, inplace=True)


        # print(ventas_cierre_mes_df)
        # Crear una lista de objetos EndMonthReportAGyMumuso a partir del DataFrame
        list_of_reports = [
            StoresEndMonthReportPenguin(**row.to_dict()) for _, row in ventas_cierre_mes_df.iterrows()
        ]

        # Formar el totales del reporte final
        total_venta_mensual_anio_anterior_iva = ventas_cierre_mes_df['venta_mensual_anio_anterior_iva'].sum()
        total_venta_mensual_anio_actual_iva = ventas_cierre_mes_df['venta_mensual_anio_actual_iva'].sum()
        
        if total_venta_mensual_anio_anterior_iva!=0:
            total_variacion_mes_porcentaje = ((total_venta_mensual_anio_actual_iva / total_venta_mensual_anio_anterior_iva)-1)*100
        else:
            total_variacion_mes_porcentaje = 0

        total_variacion_mes_efectivo = total_venta_mensual_anio_actual_iva - total_venta_mensual_anio_anterior_iva

        total_ytd_anio_anterior_iva = ventas_cierre_mes_df['ytd_anio_anterior_iva'].sum()
        total_ytd_anio_actual_iva = ventas_cierre_mes_df['ytd_anio_actual_iva'].sum()

        if total_ytd_anio_anterior_iva!=0:
            total_variacion_ytd_porcentaje = ((total_ytd_anio_actual_iva / total_ytd_anio_anterior_iva)-1)*100
        else: 
            total_variacion_ytd_porcentaje = 0
            
        total_variacion_ytd_efectivo = total_ytd_anio_actual_iva - total_ytd_anio_anterior_iva

        total_venta_objetivo = ventas_cierre_mes_df['venta_objetivo'].sum()
        if total_venta_objetivo == 0:
            total_variacion_vta_obj_porcentaje = 0
        else:
            total_variacion_vta_obj_porcentaje = ((total_venta_mensual_anio_actual_iva / total_venta_objetivo)-1)*100
    
        total_punto_equilibrio = ventas_cierre_mes_df['punto_equilibrio'].sum()


        total_existencia_tiendas = float(ventas_cierre_mes_df['existencia_tienda'].sum())
        total_venta_promedio = float(ventas_cierre_mes_df['venta_promedio'].sum())
        total_mos_tiendas = total_existencia_tiendas/total_venta_promedio


        # Calculamos el mos de los almacenes
        total_existencia_almacenes = float(suma_stock_almacenes_virtuales)
        total_mos_almacenes = (total_existencia_tiendas + total_existencia_almacenes) / total_venta_promedio

        reporte_final_cierre_mes = FinallyEndMonthReportPenguin(
            stores=list_of_reports,
            existencias_almacen_virtual = total_existencia_almacenes,

            total_venta_mensual_anio_anterior_iva=total_venta_mensual_anio_anterior_iva,
            total_venta_mensual_anio_actual_iva=total_venta_mensual_anio_actual_iva,
            total_variacion_mes_porcentaje=total_variacion_mes_porcentaje,
            total_variacion_mes_efectivo=total_variacion_mes_efectivo,
            total_ytd_anio_anterior_iva=total_ytd_anio_anterior_iva,
            total_ytd_anio_actual_iva=total_ytd_anio_actual_iva,
            total_variacion_ytd_porcentaje=total_variacion_ytd_porcentaje,
            total_variacion_ytd_efectivo=total_variacion_ytd_efectivo,

            total_venta_objetivo = total_venta_objetivo,
            total_variacion_vta_obj_porcentaje = total_variacion_vta_obj_porcentaje,
            total_punto_equilibrio = total_punto_equilibrio,

            total_existencia_tiendas=total_existencia_tiendas,
            total_mos_tiendas=total_mos_tiendas,

            total_mos_almacenes = total_mos_almacenes

        )
        
        return reporte_final_cierre_mes
    


    #? Funciones para reporte de cierre de mes detalle tienda
    # Funcion para obtener el reporte de detalle tienda de mes de todas las marcas 
    def obtener_reporte_detalle_tienda(self,nombre_marca:str, mes:str, anio:int, tipo_inventario:str):
        """Función para obtener el reporte de detalle tiendas en un periodo de mes"""

        #* Generamos la fecha en base al mes y año recibidos como parametro
        
        # Evaluamos el año recibido como parametro y lo asignamos como año actual
        if anio:
            anio_actual = int(anio)
        else:
            anio_actual = date.today().year
    
        anio_anterior = anio_actual-1
        meses_dict = {
                'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
                'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
                'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
            }
        
        mes = mes.lower()
        mes_numero = meses_dict[mes]
        _,ultimo_dia_mes_anio_actual = calendar.monthrange(anio_actual,mes_numero)
        _,ultimo_dia_mes_anio_anterior = calendar.monthrange(anio_anterior,mes_numero)

        # Definimos las fechas  de inicio y fin de mes  del año actual
        fecha_inicio_mes_anio_actual = f'{anio_actual}-{mes_numero:02}-01'
        fecha_fin_mes_anio_actual =f'{anio_actual}-{mes_numero:02}-{ultimo_dia_mes_anio_actual}'
        
        # Definimos las fechas de inicio y fin de mes del año anterior
        fecha_inicio_mes_anio_anterior = f'{anio_anterior}-{mes_numero:02}-01'
        fecha_fin_mes_anio_anterior =f'{anio_actual}-{mes_numero:02}-{ultimo_dia_mes_anio_anterior}'

        #* Consultas a la bd
        # Obtener registros de las tiendas por marca y transformalo en df
        tiendas = [t.to_dict() for t in self.shared_service.get_all_stores_by_brand_name(nombre_marca)]
        tiendas_df = pd.DataFrame(tiendas)

        # Evaluamos las marcas, puesto que algunas se agrupan de manera diferente
        if nombre_marca == 'AY GÜEY':
            # Obtenemos la informacion con los años actuales
            categorias_mes_anio_actual = self.venta_service.get_grouped_sales_by_level_by_brand(nombre_marca, fecha_inicio_mes_anio_actual,fecha_fin_mes_anio_actual,'categoria')
            categorias_mes_anio_actual_df = pd.DataFrame(categorias_mes_anio_actual)

            # Obtenemos la informacion de los años anteriores
            categorias_mes_anio_anterior = self.venta_service.get_grouped_sales_by_level_by_brand(nombre_marca, fecha_inicio_mes_anio_anterior,fecha_fin_mes_anio_anterior,'categoria')
            categorias_mes_anio_anterior_df = pd.DataFrame(categorias_mes_anio_actual)
        
        
        
        return categorias_mes_anio_actual_df.to_dict('records')