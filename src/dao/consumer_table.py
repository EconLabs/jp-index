from sqlmodel import Field, Session, SQLModel, select
from typing import Optional

class ConsumerTable(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    date: str
    ropa: float
    ropa_de_hombres: float
    ropa_de_ninos: float
    ropa_de_mujer: float
    ropa_de_ninas: float
    calzado: float
    ropa_de_bebe_y_de_infantes: float
    relojes_y_joyeria: float
    educacion_y_comunicacion: float
    materiales_educativos: float
    matricula_mensualidades_y_cuido_de_ninos: float
    correo_y_otros_servicios_postales: float
    telefonos: float
    otros_servicios_informativos: float
    alimentos_y_bebidas: float
    cereales_y_productos_de_cereales: float
    productos_horneados: float
    carne_de_res: float
    carne_de_cerdo: float
    otras_carnes: float
    carne_de_aves: float
    pescados_y_mariscos: float
    huevos: float
    productos_lacteos_y_relacionados: float
    frutas_frescas: float
    vegetales_frescos: float
    frutas_y_vegetales_elaborados: float
    jugos_de_frutas_y_vegetales_y_bebidas_sin_alcohol: float
    material_para_bebidas_incluyendo_cafe_y_te: float
    azucares_y_endulzadores: float
    grasas_aceites_y_aderezos: float
    otros_alimentos: float
    alimentos_para_consumo_fuera_del_hogar: float
    bebidas_alcoholicas_para_consumo_en_el_hogar: float
    bebidas_alcoholicas_para_consumo_fuera_del_hogar: float
    otros_articulos_y_servicios: float
    tabaco_y_productos_relacionados: float
    productos_para_el_cuidado_personal: float
    servicios_cuidado_personal: float
    servicios_personales_miscelaneos: float
    otros_gastos: float
    alojamiento: float
    alquiler_de_la_residencia_primaria: float
    alojamiento_fuera_del_hogar: float
    alquiler_equivalente_de_la_vivienda_poseida: float
    seguros_de_la_vivienda_o_de_inquilinos: float
    combustible_para_la_vivienda: float
    electricidad: float
    agua_alcantarillados_y_limpieza_de_pozos_septicos: float
    cortinas_alfombras_y_otros_similares: float
    mobiliario: float
    enseres_del_hogar: float
    otros_equipos_del_hogar: float
    herramientas_equipo_para_uso_exterior_y_articulos_de_ferreteria: float
    articulos_del_hogar: float
    servicios_para_el_hogar: float
    cuidado_medico: float
    medicinas_recetadas_y_vacunas: float
    medicinas_no_recetadas_y_equipo_medico: float
    servicios_profesionales: float
    hospitales_y_servicios_relacionados: float
    seguros_de_salud: float
    entretenimiento: float
    video_y_audio: float
    mascotas_productos_y_servicios_para_mascotas: float
    productos_deportivos: float
    fotografia: float
    otros_productos_para_entretenimiento: float
    servicios_para_el_entretenimiento: float
    libros_y_revistas: float
    transporte: float
    vehiculos_compra_y_alquiler: float
    combustible_para_motores_y_otros: float
    piezas_y_equipos_para_vehiculos_de_motor: float
    mantenimiento_y_reparacion_de_vehiculos: float
    seguros_para_vehiculos_de_motor: float
    tarifas_para_vehiculos_de_motor: float
    transporte_publico: float
    todos_los_articulos_y_servicios: float

def create_consumer_table(engine):
    SQLModel.metadata.create_all(engine)

def select_all_consumers(engine):
    with Session(engine) as session:
        statement = select(ConsumerTable)
        return session.exec(statement).all()
