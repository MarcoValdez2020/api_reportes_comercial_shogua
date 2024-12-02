import pandas as pd
from pandas.tseries.offsets import DateOffset
import numpy as np
import calendar
import json
from datetime import datetime, date

from shared.shared_service import SharedService
from ventas.venta_service import VentaService
from inventarios.inventario_service import InventarioService
from reports.report_respones import EndMonthReportAGyMumuso,FinallyEndMonthReportAGyMumuso

# Definimos la calse servicio, que sera la encargada de gestionar la logica de negocio, haciendo llamadas al repositorio
class ReportService:
    def __init__(self, shared_service: SharedService, venta_service: VentaService, inventario_service: InventarioService):
        self.shared_service = shared_service
        self.venta_service = venta_service
        self.inventario_service = inventario_service
        pass

    # Funcion para obtener el reporte de cierre de mes de Ay Güey y mumuso
    def get_month_end_report_ag_y_mu(self,nombre_marca:str, mes:str):
        """Metodo para obtener los datos de cierre de mes de las marcas Ay Güey y Mumuso"""

        # Obtener registros de las tiendas por marca y transformalo en df
        tiendas = [t.to_dict() for t in self.shared_service.get_all_stores_by_brand_name(nombre_marca)]
        tiendas_df = pd.DataFrame(tiendas)

        # Obtener los inventarios de todas las tiendas
        inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name(nombre_marca)
        inventarios_tiendas_dict = self.inventario_service.transform_store_inventories_total_stock(inventarios_tiendas)
        inventarios_tiendas_df = pd.DataFrame(inventarios_tiendas_dict)


        # Obtener todas las ventas de inicio a fin del mes seleccionado
        anio_actual = date.today().year
        anio_anterior = date.today().year-1

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
        
        reporte_cierre_mes = self.fusionar_dataframes_cierre_mes_ag_y_mu(ventas_mes_anterior_df,ventas_mes_actual_df,
                                                                        ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual,
                                                                        tiendas_df,inventarios_tiendas_df, ventas_promedio_df)
            
        return reporte_cierre_mes
    

    # Funcion para concatenar el df de cierre de mes ag y mumuso
    def fusionar_dataframes_cierre_mes_ag_y_mu(self, ventas_mes_anterior_df:pd.DataFrame, ventas_mes_actual_df:pd.DataFrame,
                                                ventas_ytd_anio_anterior_df:pd.DataFrame,ventas_ytd_anio_actual:pd.DataFrame,
                                                tiendas_df:pd.DataFrame, inventarios_tiendas_df:pd.DataFrame, ventas_promedio_df: pd.DataFrame):
        
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
        # Fusionar mes con mes
        ventas_mes_df = pd.merge(ventas_mes_anterior_df,ventas_mes_actual_df, on='whscode', how='outer')

        # Fusionar año con año
        ventas_ytd_df = pd.merge(ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual, on='whscode', how='outer')

        # Fusionar meses con el ytd
        ventas_cierre_mes_df = pd.merge(ventas_mes_df,ventas_ytd_df, on='whscode', how='outer')

        # Calcular las variaciones
        ventas_cierre_mes_df = ventas_cierre_mes_df.fillna(0)
        
        ventas_cierre_mes_df['variacion_mes_porcentaje'] = np.where(ventas_cierre_mes_df['venta_mensual_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] / ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_mes_efectivo'] =  ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] - ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']

        ventas_cierre_mes_df['variacion_ytd_porcentaje'] = np.where(ventas_cierre_mes_df['ytd_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['ytd_anio_actual_iva'] / ventas_cierre_mes_df['ytd_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_ytd_efectivo']  =   ventas_cierre_mes_df['ytd_anio_actual_iva'] - ventas_cierre_mes_df['ytd_anio_anterior_iva']


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
    def get_month_end_report_tous_hm_y_ricodeli(self, nombre_marca:str, mes:str):
        """Metodo para obtener los datos de cierre de mes de Tous Hetal Mevy y Ricodeli"""

        # Obtener registros de las tiendas por marca y transformalo en df
        tiendas = [t.to_dict() for t in self.shared_service.get_all_stores_by_brand_name(nombre_marca)]
        tiendas_df = pd.DataFrame(tiendas)

        # Obtener los inventarios de todas las tiendas
        inventarios_tiendas = self.inventario_service.get_store_inventories_total_stock_by_brand_name(nombre_marca)
        inventarios_tiendas_dict = self.inventario_service.transform_store_inventories_total_stock(inventarios_tiendas)
        inventarios_tiendas_df = pd.DataFrame(inventarios_tiendas_dict)

        # Obtener la suma de stock de los almacenes virtuales
        inventarios_almacenes_virtuales = self.inventario_service.get_virtual_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
        suma_stock_almacenes_virtuales = inventarios_almacenes_virtuales[0]

        # Obtener la suma de los inventarios fisicos de los almacenes
        inventarios_almacenes_fisicos = self.inventario_service.get_physical_warehouses_inventories_total_stock_by_brand_name(nombre_marca)
        
        # En caso de no tener existencia los almacenes fisicos que son dos solamente retornar el diccionario con valores vacios
        if len(inventarios_almacenes_fisicos) == 0:
            inventarios_almacenes_fisicos = {
                'CDCUN01':0,
                'CDMX01': 0
            }

                # Obtener todas las ventas de inicio a fin del mes seleccionado
        anio_actual = date.today().year
        anio_anterior = date.today().year-1

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

        reporte_cierre_mes = self.fusionar_dataframes_cierre_mes_tous(ventas_mes_anterior_df,ventas_mes_actual_df,
                                                                        ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual,
                                                                        tiendas_df,inventarios_tiendas_df, ventas_promedio_df)



        return reporte_cierre_mes
    

    def fusionar_dataframes_cierre_mes_tous(self, ventas_mes_anterior_df:pd.DataFrame, ventas_mes_actual_df:pd.DataFrame,
                                                ventas_ytd_anio_anterior_df:pd.DataFrame,ventas_ytd_anio_actual:pd.DataFrame,
                                                tiendas_df:pd.DataFrame, inventarios_tiendas_df:pd.DataFrame, ventas_promedio_df: pd.DataFrame):
        
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
        # Fusionar mes con mes
        ventas_mes_df = pd.merge(ventas_mes_anterior_df,ventas_mes_actual_df, on='whscode', how='outer')

        # Fusionar año con año
        ventas_ytd_df = pd.merge(ventas_ytd_anio_anterior_df,ventas_ytd_anio_actual, on='whscode', how='outer')

        # Fusionar meses con el ytd
        ventas_cierre_mes_df = pd.merge(ventas_mes_df,ventas_ytd_df, on='whscode', how='outer')

        # Calcular las variaciones
        ventas_cierre_mes_df = ventas_cierre_mes_df.fillna(0)
        
        ventas_cierre_mes_df['variacion_mes_porcentaje'] = np.where(ventas_cierre_mes_df['venta_mensual_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] / ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_mes_efectivo'] =  ventas_cierre_mes_df['venta_mensual_anio_actual_iva'] - ventas_cierre_mes_df['venta_mensual_anio_anterior_iva']

        ventas_cierre_mes_df['variacion_ytd_porcentaje'] = np.where(ventas_cierre_mes_df['ytd_anio_anterior_iva'] != 0,
                                                        (((ventas_cierre_mes_df['ytd_anio_actual_iva'] / ventas_cierre_mes_df['ytd_anio_anterior_iva']) - 1)*100),0)
        
        ventas_cierre_mes_df['variacion_ytd_efectivo']  =   ventas_cierre_mes_df['ytd_anio_actual_iva'] - ventas_cierre_mes_df['ytd_anio_anterior_iva']


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
            total_existencia=total_existencia,
            total_mos=total_mos
        )
        
        return reporte_final_cierre_mes
    

        