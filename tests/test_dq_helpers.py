"""
Tests unitarios para funciones de utilidad de calidad de datos (DQ).

Cubre: get_path, schema_fingerprint
Estas funciones son puras (sin Spark ni dbutils) y se pueden
ejecutar con: pytest tests/test_dq_helpers.py
"""

import json
import hashlib
import pytest

# Funciones bajo test (copiadas del notebook 01_data_quality_bronze.py)

STORAGE_ACCOUNT = "dataecoazul"


def get_path(container, path=""):
    """Construye la URL wasbs:// completa para acceder a Azure Blob Storage."""
    base = f"wasbs://{container}@{STORAGE_ACCOUNT}.blob.core.windows.net"
    return f"{base}/{path.lstrip('/')}" if path else base


def schema_fingerprint(schema) -> str:
    """Calcula el hash SHA256 del esquema Spark serializado como JSON ordenado."""
    j = json.dumps(schema.jsonValue(), sort_keys=True)
    return hashlib.sha256(j.encode("utf-8")).hexdigest()


# Helpers de test 

class FakeSchema:
    """Simula un objeto de schema de Spark con el método jsonValue()."""
    def __init__(self, data: dict):
        self._data = data

    def jsonValue(self):
        return self._data


# Tests: get_path 

class TestGetPath:

    def test_solo_container_devuelve_base(self):
        resultado = get_path("bronze")
        assert resultado == "wasbs://bronze@dataecoazul.blob.core.windows.net"

    def test_con_path_simple(self):
        resultado = get_path("bronze", "stormglass/forecast")
        assert resultado == "wasbs://bronze@dataecoazul.blob.core.windows.net/stormglass/forecast"

    def test_path_con_slash_inicial_se_elimina(self):
        resultado = get_path("bronze", "/stormglass/forecast")
        assert resultado == "wasbs://bronze@dataecoazul.blob.core.windows.net/stormglass/forecast"

    def test_container_silver(self):
        resultado = get_path("silver")
        assert "silver" in resultado
        assert STORAGE_ACCOUNT in resultado

    def test_path_con_particiones(self):
        resultado = get_path("bronze", "stormglass/year=2024/month=01/day=15")
        assert "year=2024/month=01/day=15" in resultado

    def test_path_vacio_equivale_a_sin_path(self):
        assert get_path("bronze", "") == get_path("bronze")


# Tests: schema_fingerprint 

class TestSchemaFingerprint:

    def test_devuelve_string_hex_de_64_caracteres(self):
        schema = FakeSchema({"fields": [{"name": "id", "type": "string"}]})
        fp = schema_fingerprint(schema)
        assert isinstance(fp, str)
        assert len(fp) == 64
        assert all(c in "0123456789abcdef" for c in fp)

    def test_mismo_schema_produce_mismo_fingerprint(self):
        data = {"fields": [{"name": "punto_id", "type": "string"}, {"name": "ts", "type": "timestamp"}]}
        fp1 = schema_fingerprint(FakeSchema(data))
        fp2 = schema_fingerprint(FakeSchema(data))
        assert fp1 == fp2

    def test_schemas_distintos_producen_fingerprints_distintos(self):
        schema_a = FakeSchema({"fields": [{"name": "campo_a", "type": "string"}]})
        schema_b = FakeSchema({"fields": [{"name": "campo_b", "type": "double"}]})
        assert schema_fingerprint(schema_a) != schema_fingerprint(schema_b)

    def test_orden_de_claves_no_afecta_fingerprint(self):
        # sort_keys=True garantiza que el orden de inserción no importa
        schema_a = FakeSchema({"type": "string", "name": "campo"})
        schema_b = FakeSchema({"name": "campo", "type": "string"})
        assert schema_fingerprint(schema_a) == schema_fingerprint(schema_b)

    def test_schema_vacio_no_lanza_excepcion(self):
        fp = schema_fingerprint(FakeSchema({}))
        assert len(fp) == 64
