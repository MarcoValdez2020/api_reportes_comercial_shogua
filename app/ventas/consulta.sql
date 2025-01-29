WITH variables AS (
    SELECT
        '2024-01-01'::date AS fecha_inicio_anio_anterior,
        '2024-01-31'::date AS fecha_fin_anio_anterior,
        '2025-01-01'::date AS fecha_inicio_anio_actual,
        '2025-01-31'::date AS fecha_fin_anio_actual,
        ARRAY['MUCUNT3', '']::text[] AS whscodes,
        'MUMUSO'::text AS nombre_marca,
        '2024-12-01'::date AS fecha_cierre_anio_anterior,
        '2024-01-01'::date AS fecha_inicio_periodo_ventas_prom,
        '2024-12-31'::date AS fecha_fin_periodo_ventas_prom
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
    WHERE tienda.whscode = ANY (variables.whscodes)
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
    FROM inventario_tienda
    JOIN producto ON producto.id_producto = inventario_tienda.id_producto
    JOIN tienda ON tienda.whscode = inventario_tienda.whscode
    JOIN marca ON marca.id_marca = tienda.id_marca
    CROSS JOIN variables
    WHERE tienda.whscode = ANY (variables.whscodes)
      AND marca.nombre = variables.nombre_marca
    GROUP BY producto.departamento

    UNION ALL

    SELECT 
        'CATEGORIA' AS nivel,
        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') AS key,
        SUM(existencia) AS existencia_tienda
    FROM inventario_tienda
    JOIN producto ON producto.id_producto = inventario_tienda.id_producto
    JOIN tienda ON tienda.whscode = inventario_tienda.whscode
    JOIN marca ON marca.id_marca = tienda.id_marca
    CROSS JOIN variables
    WHERE tienda.whscode = ANY (variables.whscodes)
      AND marca.nombre = variables.nombre_marca
    GROUP BY producto.departamento, producto.categoria

    UNION ALL

    SELECT 
        'SUBCATEGORIA' AS nivel,
        departamento || '-' || COALESCE(categoria, 'SIN CATEGORIA') || '-' || COALESCE(subcategoria, 'SIN SUBCATEGORIA') AS key,
        SUM(existencia) AS existencia_tienda
    FROM inventario_tienda
    JOIN producto ON producto.id_producto = inventario_tienda.id_producto
    JOIN tienda ON tienda.whscode = inventario_tienda.whscode
    JOIN marca ON marca.id_marca = tienda.id_marca
    CROSS JOIN variables
    WHERE tienda.whscode = ANY (variables.whscodes)
      AND marca.nombre = variables.nombre_marca
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
    WHERE tienda.whscode = ANY (variables.whscodes)
      AND marca.nombre = variables.nombre_marca
      AND historial_inventario_tienda.fecha = variables.fecha_cierre_anio_anterior
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
    WHERE tienda.whscode = ANY (variables.whscodes)
      AND marca.nombre = variables.nombre_marca
      AND historial_inventario_tienda.fecha = variables.fecha_cierre_anio_anterior
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
    WHERE tienda.whscode = ANY (variables.whscodes)
      AND marca.nombre = variables.nombre_marca
      AND historial_inventario_tienda.fecha = variables.fecha_cierre_anio_anterior
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
    WHERE tienda.whscode = ANY (variables.whscodes)
      AND marca.nombre = variables.nombre_marca
      AND venta.fecha BETWEEN variables.fecha_inicio_anio_actual AND variables.fecha_fin_anio_actual
),
InventariosTotalesPorTienda AS (
    SELECT 
        SUM(inventario_tienda.existencia) AS total_existencias_tiendas
    FROM inventario_tienda
    JOIN tienda ON tienda.whscode = inventario_tienda.whscode
    JOIN marca ON marca.id_marca = tienda.id_marca
    CROSS JOIN variables
    WHERE tienda.whscode = ANY (variables.whscodes)
      AND marca.nombre = variables.nombre_marca
      -- where de historial inventario si es cierre-mes
      --AND venta.fecha BETWEEN variables.fecha_inicio_anio_actual AND variables.fecha_fin_anio_actual
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
	  WHERE marca.nombre = variables.nombre_marca
	      AND venta.fecha >= DATE_TRUNC('month', variables.fecha_inicio_periodo_ventas_prom)
	      AND venta.fecha <= variables.fecha_fin_periodo_ventas_prom
	      AND (venta.venta_neta_sin_iva < -1 OR venta.venta_neta_sin_iva > 1)
	      AND venta.whscode = ANY (SELECT unnest(whscodes) FROM variables)
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
	CROSS JOIN variables
	WHERE tienda.whscode = ANY (variables.whscodes)
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
    COALESCE(h.existencia_tienda_cm_aant, 0) AS existencia_tienda_cierre_aant,
    COALESCE(i.existencia_tienda, 0) AS existencia_tienda,
    ROUND(COALESCE((CAST(i.existencia_tienda AS numeric) / NULLIF(CAST(h.existencia_tienda_cm_aant AS numeric), 0)) - 1, 0), 2) AS variacion_prc_inv,
    --COALESCE(vt.total_ventas_tienda, 0) AS total_ventas_tienda,
    ROUND(COALESCE(t.cantidad_anio_actual, 0) / NULLIF(vt.total_ventas_tienda_pzs, 0), 2) AS porcentaje_participacion_venta_pzs,
    --COALESCE(invtot.total_existencias_tiendas, 0) AS total_existencias_tiendas,
	ROUND(COALESCE(CAST(existencia_tienda AS numeric) / NULLIF(CAST(total_existencias_tiendas AS numeric), 0), 0), 2) AS porcentaje_participacion_inv_pzs,
    ROUND(COALESCE(t.iva_anio_actual, 0) / NULLIF(vt.total_ventas_efectivo, 0) * spt.venta_objetivo, 2) AS presupuesto,
	--Calculo del mos de tienda
	ROUND(COALESCE((CAST(i.existencia_tienda AS numeric) / NULLIF(CAST(pv.venta_promedio AS numeric), 0)), 0), 2) AS mos_tienda

FROM TotalesVariacionesPorNivel t
FULL OUTER JOIN InventarioTiendaPorNivel i ON t.nivel = i.nivel AND t.key = i.key
FULL OUTER JOIN InventarioCierreAnioAnteriorPN h ON t.nivel = h.nivel AND t.key = h.key
FULL OUTER JOIN TotalesPromedioVentaPorNivel pv ON t.key = pv.key

CROSS JOIN VentasTotalesPorTienda vt
CROSS JOIN InventariosTotalesPorTienda invtot
CROSS JOIN SumaPresupuestosTiendas spt
WHERE t.key ILIKE 'BELLEZA%'
  AND NOT (cantidad_anio_actual<= 0 AND i.existencia_tienda<= 0);