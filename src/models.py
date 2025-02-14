import duckdb


def get_conn(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path)


def init_indicators_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)

    # Create sequence for primary key
    conn.sql("DROP SEQUENCE IF EXISTS indicators_sequence;")
    conn.sql("CREATE SEQUENCE indicators_sequence START 1;")

    # Create Indicators table
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "indicatorstable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('indicators_sequence'),
            date TEXT,
            indice_de_actividad_economica FLOAT,
            encuesta_de_grupo_trabajador_ajustada_estacionalmente FLOAT,
            encuesta_de_grupo_trabajador FLOAT,
            encuesta_de_establecimientos_ajustados_estacionalmente FLOAT,
            encuesta_de_establecimientos FLOAT,
            indicadores_de_turismo FLOAT,
            indicadores_de_construccion FLOAT,
            indicadores_de_ingresos_netos FLOAT,
            indicadores_de_energia_electrica FLOAT,
            indicadores_de_comercio_exterior FLOAT,
            indicadores_de_quiebras FLOAT,
            indicadores_de_ventas_al_detalle_a_precios_corrientes FLOAT,
            precios_promedios_mensuales_de_gasolina_al_detal_en_puerto_rico FLOAT,
            indice_de_precios_al_consumidor_2006_100 FLOAT,
            indicadores_de_transportacion FLOAT,
            indices_coincidentes_de_actividad_economica FLOAT,
            encuesta_de_establecimientos_manufactura FLOAT
        );
        """
    )


def init_consumer_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)

    # Create sequence for primary key
    conn.sql("DROP SEQUENCE IF EXISTS consumer_sequence;")
    conn.sql("CREATE SEQUENCE consumer_sequence START 1;")

    # Create Consumer table
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "consumertable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('consumer_sequence'),
            date DATETIME,
            year INTEGER,
            month INTEGER,
            quarter INTEGER,
            fiscal INTEGER,
            ropa FLOAT,
            ropa_de_hombres FLOAT,
            ropa_de_ninos FLOAT,
            ropa_de_mujer FLOAT,
            ropa_de_ninas FLOAT,
            calzado FLOAT,
            ropa_de_bebe_y_de_infantes FLOAT,
            relojes_y_joyeria FLOAT,
            educacion_y_comunicacion FLOAT,
            materiales_educativos FLOAT,
            matricula_mensualidades_y_cuido_de_ninos FLOAT,
            correo_y_otros_servicios_postales FLOAT,
            telefonos FLOAT,
            otros_servicios_informativos FLOAT,
            alimentos_y_bebidas FLOAT,
            cereales_y_productos_de_cereales FLOAT,
            productos_horneados FLOAT,
            carne_de_res FLOAT,
            carne_de_cerdo FLOAT,
            otras_carnes FLOAT,
            carne_de_aves FLOAT,
            pescados_y_mariscos FLOAT,
            huevos FLOAT,
            productos_lacteos_y_relacionados FLOAT,
            frutas_frescas FLOAT,
            vegetales_frescos FLOAT,
            frutas_y_vegetales_elaborados FLOAT,
            jugos_de_frutas_y_vegetales_y_bebidas_sin_alcohol FLOAT,
            material_para_bebidas_incluyendo_cafe_y_te FLOAT,
            azucares_y_endulzadores FLOAT,
            grasas_aceites_y_aderezos FLOAT,
            otros_alimentos FLOAT,
            alimentos_para_consumo_fuera_del_hogar FLOAT,
            bebidas_alcoholicas_para_consumo_en_el_hogar FLOAT,
            bebidas_alcoholicas_para_consumo_fuera_del_hogar FLOAT,
            otros_articulos_y_servicios FLOAT,
            tabaco_y_productos_relacionados FLOAT,
            productos_para_el_cuidado_personal FLOAT,
            servicios_cuidado_personal FLOAT,
            servicios_personales_miscelaneos FLOAT,
            otros_gastos FLOAT,
            alojamiento FLOAT,
            alquiler_de_la_residencia_primaria FLOAT,
            alojamiento_fuera_del_hogar FLOAT,
            alquiler_equivalente_de_la_vivienda_poseida FLOAT,
            seguros_de_la_vivienda_o_de_inquilinos FLOAT,
            combustible_para_la_vivienda FLOAT,
            electricidad FLOAT,
            agua_alcantarillados_y_limpieza_de_pozos_septicos FLOAT,
            cortinas_alfombras_y_otros_similares FLOAT,
            mobiliario FLOAT,
            enseres_del_hogar FLOAT,
            otros_equipos_del_hogar FLOAT,
            herramientas_equipo_para_uso_exterior_y_articulos_de_ferreteria FLOAT,
            articulos_del_hogar FLOAT,
            servicios_para_el_hogar FLOAT,
            cuidado_medico FLOAT,
            medicinas_recetadas_y_vacunas FLOAT,
            medicinas_no_recetadas_y_equipo_medico FLOAT,
            servicios_profesionales FLOAT,
            hospitales_y_servicios_relacionados FLOAT,
            seguros_de_salud FLOAT,
            entretenimiento FLOAT,
            video_y_audio FLOAT,
            mascotas_productos_y_servicios_para_mascotas FLOAT,
            productos_deportivos FLOAT,
            fotografia FLOAT,
            otros_productos_para_entretenimiento FLOAT,
            servicios_para_el_entretenimiento FLOAT,
            libros_y_revistas FLOAT,
            transporte FLOAT,
            vehiculos_compra_y_alquiler FLOAT,
            combustible_para_motores_y_otros FLOAT,
            piezas_y_equipos_para_vehiculos_de_motor FLOAT,
            mantenimiento_y_reparacion_de_vehiculos FLOAT,
            seguros_para_vehiculos_de_motor FLOAT,
            tarifas_para_vehiculos_de_motor FLOAT,
            transporte_publico FLOAT,
            todos_los_articulos_y_servicios FLOAT
        );
        """
    )

def init_activity_table(db_path: str) -> None:
    conn = get_conn(db_path=db_path)

    # Create sequence for primary key
    conn.sql("DROP SEQUENCE IF EXISTS activity_sequence;")
    conn.sql("CREATE SEQUENCE activity_sequence START 1;")

    # Create Indicators table
    conn.sql(
        """
        CREATE TABLE IF NOT EXISTS "activitytable" (
            id INTEGER PRIMARY KEY DEFAULT nextval('activity_sequence'),
            date DATETIME,
            index FLOAT
        );
        """
    )