"""
Tests unitarios para la fórmula del semáforo de riesgo marino.

La fórmula está implementada en gold.condiciones_actuales_zona y en
02_gold_condiciones_actuales_zona.py. Es determinista y no requiere Spark.

Ejecutar con: pytest tests/test_risk_score.py
"""

import pytest

#  Lógica bajo test (extraída de 02_gold_condiciones_actuales_zona.py) 

def calcular_risk_score(wave_height_m, wind_speed_knots, current_speed_ms, visibility_km):
    """
    Calcula el risk_score numérico de condiciones marinas.

    Fórmula:
        score = wave_height_m * 10 + wind_speed_knots + current_speed_ms * 5
        + 20 si visibility_km < 2

    Returns:
        float — puntuación de riesgo (escala 0-200)
    """
    score = wave_height_m * 10 + wind_speed_knots + current_speed_ms * 5
    if visibility_km < 2:
        score += 20
    return score


def clasificar_riesgo(wave_height_m, wind_speed_knots, current_speed_ms, visibility_km):
    """
    Clasifica las condiciones marinas en SEGURO / PRECAUCION / PELIGRO.

    Umbrales directos (independientes del score):
        - wave_height_m > 2.5  → PELIGRO
        - wind_speed_knots > 25 → PELIGRO

    Por score:
        - score < 20  → SEGURO
        - score < 40  → PRECAUCION
        - score >= 40 → PELIGRO
    """
    if wave_height_m > 2.5 or wind_speed_knots > 25:
        return "PELIGRO"
    score = calcular_risk_score(wave_height_m, wind_speed_knots, current_speed_ms, visibility_km)
    if score >= 40:
        return "PELIGRO"
    if score >= 20:
        return "PRECAUCION"
    return "SEGURO"


# Tests: calcular_risk_score 

class TestCalcularRiskScore:

    def test_condiciones_calmas_score_bajo(self):
        score = calcular_risk_score(
            wave_height_m=0.5, wind_speed_knots=5,
            current_speed_ms=0.1, visibility_km=10
        )
        assert score < 20

    def test_penalizacion_visibilidad_baja(self):
        score_buena = calcular_risk_score(0.5, 5, 0.1, visibility_km=10)
        score_mala  = calcular_risk_score(0.5, 5, 0.1, visibility_km=1)
        assert score_mala - score_buena == 20

    def test_sin_penalizacion_visibilidad_exactamente_2km(self):
        # límite: < 2 activa la penalización, exactamente 2 no
        score = calcular_risk_score(0.0, 0.0, 0.0, visibility_km=2)
        assert score == 0

    def test_oleaje_domina_el_score(self):
        score = calcular_risk_score(
            wave_height_m=3.0, wind_speed_knots=0,
            current_speed_ms=0.0, visibility_km=10
        )
        assert score == 30  # 3.0 * 10

    def test_score_es_suma_de_componentes(self):
        score = calcular_risk_score(1.0, 10.0, 2.0, visibility_km=10)
        esperado = 1.0 * 10 + 10.0 + 2.0 * 5
        assert score == pytest.approx(esperado)


#  Tests: clasificar_riesgo 

class TestClasificarRiesgo:

    def test_condiciones_calmas_es_seguro(self):
        assert clasificar_riesgo(0.3, 5, 0.1, 15) == "SEGURO"

    def test_condiciones_moderadas_es_precaucion(self):
        # score = 1.5*10 + 7 + 0.5*5 = 24.5 → PRECAUCION
        assert clasificar_riesgo(1.5, 7, 0.5, 10) == "PRECAUCION"

    def test_oleaje_alto_dispara_peligro_directo(self):
        # wave > 2.5 → PELIGRO sin importar el score
        assert clasificar_riesgo(2.6, 5, 0.1, 10) == "PELIGRO"

    def test_viento_alto_dispara_peligro_directo(self):
        # wind > 25 knots → PELIGRO sin importar el score
        assert clasificar_riesgo(0.5, 26, 0.1, 10) == "PELIGRO"

    def test_score_alto_sin_umbrales_directos_es_peligro(self):
        # wave=2.0, wind=20, current=1.0 → score = 20+20+5 = 45 → PELIGRO
        assert clasificar_riesgo(2.0, 20, 1.0, 10) == "PELIGRO"

    def test_visibilidad_baja_puede_elevar_a_peligro(self):
        # sin visibilidad: score=15+5+0+20=40 → PELIGRO
        assert clasificar_riesgo(1.5, 5, 0.0, 1) == "PELIGRO"

    def test_limite_seguro_precaucion(self):
        # score exactamente 20 → PRECAUCION
        assert clasificar_riesgo(2.0, 0, 0.0, 10) == "PRECAUCION"

    def test_oleaje_exactamente_en_umbral_no_es_peligro_directo(self):
        # 2.5 no es > 2.5, se evalúa por score
        resultado = clasificar_riesgo(2.5, 5, 0.0, 10)
        # score = 25+5 = 30 → PRECAUCION
        assert resultado == "PRECAUCION"
