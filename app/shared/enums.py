from enum import Enum

# Definición de los ENUMs
class TipoTienda(str, Enum):
    PLAZA = "PLAZA"
    AEROPUERTO = "AEROPUERTO"

class EstadoOperativo(str, Enum):
    ABIERTA = "ABIERTA"
    CERRADA = "CERRADA"

class Comparabilidad(str, Enum):
    COMPARABLE = "COMPARABLE"
    NO_COMPARABLE = "NO_COMPARABLE"

class SistemaERP(str, Enum):
    SAP = "SAP"
    PROSCAI = "PROSCAI"
    FBH = "FBH"

class GiroMarca(str, Enum):
    RETAIL = "RETAIL"
    ALIMENTOS_Y_BEBIDAS = "ALIMENTOS Y BEBIDAS"
