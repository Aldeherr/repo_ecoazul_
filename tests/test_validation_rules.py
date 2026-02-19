"""
Tests unitarios para las reglas de validación de contenido Bronze.

Cubre: validate_marea_content, validate_stormglass_content
Los DataFrames de Spark se simulan con objetos mock simples.

Ejecutar con: pytest tests/test_validation_rules.py
"""

import pytest

#  Mock de DataFrame Spark 

class MockDF:
    """
    Simula un DataFrame de Spark con filter() y count().
    filter() devuelve el mismo mock (ignora la condición),
    count() devuelve el valor configurado en la construcción.
    """
    def __init__(self, count_val: int):
        self._count_val = count_val

    def filter(self, *args, **kwargs):
        return self

    def count(self):
        return self._count_val


#  Funciones bajo test (copiadas de 01_data_quality_bronze.py) 

def validate_marea_content(metrics, expected_puntos=30, expected_points=48, min_range_h=47.0):
    violations = []

    if metrics["puntos_cnt"] != expected_puntos:
        violations.append({
            "rule": "PUNTOS_COUNT",
            "found": metrics["puntos_cnt"],
            "expected": expected_puntos
        })

    bad_cnt = metrics["rng"].filter(None).count()
    if bad_cnt > 0:
        violations.append({
            "rule": "RANGE_OR_POINTS",
            "bad_puntos": bad_cnt,
            "expected_points": expected_points,
            "min_range_h": min_range_h
        })

    dup_cnt = metrics["dup_heights"].count()
    if dup_cnt > 0:
        violations.append({
            "rule": "DUPLICATES_HEIGHTS",
            "dup_keys": dup_cnt
        })

    return violations


def validate_stormglass_content(metrics, expected_puntos=30, expected_points=48, min_range_h=47.0):
    violations = []

    if metrics["puntos_cnt"] != expected_puntos:
        violations.append({
            "rule": "PUNTOS_COUNT",
            "found": metrics["puntos_cnt"],
            "expected": expected_puntos
        })

    bad_cnt = metrics["rng"].filter(None).count()
    if bad_cnt > 0:
        violations.append({
            "rule": "RANGE_OR_POINTS",
            "bad_puntos": bad_cnt,
            "expected_points": expected_points,
            "min_range_h": min_range_h
        })

    dup_cnt = metrics["dup_hours"].count()
    if dup_cnt > 0:
        violations.append({
            "rule": "DUPLICATES_HOURS",
            "dup_keys": dup_cnt
        })

    return violations


# Tests: validate_marea_content 

class TestValidateMareaContent:

    def _ok_metrics(self):
        return {
            "puntos_cnt": 30,
            "rng": MockDF(0),
            "dup_heights": MockDF(0)
        }

    def test_datos_correctos_sin_violaciones(self):
        assert validate_marea_content(self._ok_metrics()) == []

    def test_puntos_insuficientes_genera_violacion(self):
        metrics = self._ok_metrics()
        metrics["puntos_cnt"] = 25
        violations = validate_marea_content(metrics)
        reglas = [v["rule"] for v in violations]
        assert "PUNTOS_COUNT" in reglas

    def test_puntos_en_exceso_genera_violacion(self):
        metrics = self._ok_metrics()
        metrics["puntos_cnt"] = 31
        violations = validate_marea_content(metrics)
        assert any(v["rule"] == "PUNTOS_COUNT" for v in violations)

    def test_rango_horario_insuficiente_genera_violacion(self):
        metrics = self._ok_metrics()
        metrics["rng"] = MockDF(3)  # 3 puntos con rango insuficiente
        violations = validate_marea_content(metrics)
        assert any(v["rule"] == "RANGE_OR_POINTS" for v in violations)

    def test_duplicados_generan_violacion(self):
        metrics = self._ok_metrics()
        metrics["dup_heights"] = MockDF(5)
        violations = validate_marea_content(metrics)
        assert any(v["rule"] == "DUPLICATES_HEIGHTS" for v in violations)

    def test_multiples_problemas_generan_multiples_violaciones(self):
        metrics = {
            "puntos_cnt": 20,
            "rng": MockDF(2),
            "dup_heights": MockDF(4)
        }
        violations = validate_marea_content(metrics)
        assert len(violations) == 3

    def test_expected_puntos_configurable(self):
        metrics = self._ok_metrics()
        metrics["puntos_cnt"] = 10
        violations = validate_marea_content(metrics, expected_puntos=10)
        assert violations == []


#  Tests: validate_stormglass_content 

class TestValidateStormglassContent:

    def _ok_metrics(self):
        return {
            "puntos_cnt": 30,
            "rng": MockDF(0),
            "dup_hours": MockDF(0)
        }

    def test_datos_correctos_sin_violaciones(self):
        assert validate_stormglass_content(self._ok_metrics()) == []

    def test_puntos_incorrectos_genera_violacion(self):
        metrics = self._ok_metrics()
        metrics["puntos_cnt"] = 0
        violations = validate_stormglass_content(metrics)
        assert any(v["rule"] == "PUNTOS_COUNT" for v in violations)

    def test_horas_duplicadas_generan_violacion(self):
        metrics = self._ok_metrics()
        metrics["dup_hours"] = MockDF(2)
        violations = validate_stormglass_content(metrics)
        assert any(v["rule"] == "DUPLICATES_HOURS" for v in violations)

    def test_sin_duplicados_no_genera_violacion(self):
        metrics = self._ok_metrics()
        metrics["dup_hours"] = MockDF(0)
        violations = validate_stormglass_content(metrics)
        assert not any(v["rule"] == "DUPLICATES_HOURS" for v in violations)
