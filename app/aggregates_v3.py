"""Aggregates v3: consolida todo (5 años + enriquecido + PJUD) para el dashboard.

Produce static/aggregates.json con:
- resumen (global 2026)
- por_region / por_comuna / por_sexo / por_edad / por_edad_sexo / por_decil / por_nse (2026)
- por_vehiculos / por_propiedades / condicion_propietario (2026)
- por_monto_bucket / por_universidad / por_seniority / por_tier / top_industrias / top_empresas (2026)
- perfiles (categorización maestra 2026)
- evolucion (2022-2026): deudores, cartera UTM/CLP/USD, deuda avg/med, por universidad por año
- flujos (2022→2026): nuevos, pagaron_total, acumularon_intereses, pagaron_parcial
- justicia: distribución de causas (civil/laboral) con k-anonimato por cortes
- xray_universidades: dict {universidad_canon: {resumen, evolucion, demografia, justicia}}
"""
import duckdb
import json
from pathlib import Path

DATA = Path("/Users/antonio/deudores-fscu/data")
OUT = Path("/Users/antonio/deudores-fscu/public/static/aggregates.json")
K = 10
UTM_CLP = 69889
USD_CLP = 886.32
UTM_USD = UTM_CLP / USD_CLP

MACROZONAS = {
    "Arica Y Parinacota": "Norte", "Tarapacá": "Norte", "Antofagasta": "Norte", "Atacama": "Norte",
    "Coquimbo": "Centro", "Valparaíso": "Centro",
    "Metropolitana": "Metropolitana",
    "O’Higgins": "Centro sur", "Maule": "Centro sur", "Ñuble": "Centro sur", "Biobío": "Centro sur",
    "La Araucanía": "Sur", "Los Ríos": "Sur", "Los Lagos": "Sur",
    "Aysén": "Austral", "Magallanes": "Austral",
}

con = duckdb.connect(":memory:")

# ═══ Cargar datasets ═══
# Enriquecido con patrimonio + LinkedIn (2026)
con.execute(f"""CREATE VIEW enriched AS SELECT * FROM read_parquet('{DATA}/deudores_enriched.parquet')""")
# Nómina consolidada histórica
con.execute(f"""CREATE VIEW nominas AS SELECT * FROM read_parquet('{DATA}/nominas_consolidado.parquet')""")
# 2026 con monto
con.execute(f"""CREATE VIEW nom2026 AS SELECT rut_dv, monto_utm, universidad_canon FROM nominas WHERE year=2026""")
# PJUD
con.execute(f"""CREATE VIEW pjud AS SELECT * FROM read_parquet('{DATA}/deudores_pjud.parquet')""")

# Join enriched + 2026 monto + PJUD
con.execute("""
CREATE TABLE full_t AS
SELECT
    e.*,
    n.monto_utm, n.universidad_canon AS universidad_principal,
    COALESCE(p.n_civil, 0) AS n_civil,
    COALESCE(p.n_civil_ddo, 0) AS n_civil_ddo,
    COALESCE(p.n_civil_dte, 0) AS n_civil_dte,
    COALESCE(p.n_laboral, 0) AS n_laboral,
    COALESCE(p.n_laboral_ddo, 0) AS n_laboral_ddo,
    COALESCE(p.en_pjud, FALSE) AS en_pjud,
    p.ultimo_year_civil
FROM enriched e
LEFT JOIN nom2026 n ON e.rut_dv = n.rut_dv
LEFT JOIN pjud p ON e.rut_dv = p.rut_dv
""")
m = con.execute("SELECT COUNT(*), SUM(CASE WHEN en_pjud THEN 1 ELSE 0 END), SUM(CASE WHEN monto_utm IS NOT NULL THEN 1 ELSE 0 END) FROM full_t").fetchone()
print(f"full_t: {m[0]:,} · con PJUD: {m[1]:,} · con monto 2026: {m[2]:,}")

def table(sql, k_col="n"):
    rows = con.execute(sql).fetchdf().to_dict(orient="records")
    return [{k: (None if v != v else v) for k, v in r.items()} for r in rows if r.get(k_col, 0) >= K]

out = {}

# ═══ RESUMEN ═══
r = con.execute("""
SELECT
    COUNT(*) total,
    SUM(CASE WHEN en_linkedin THEN 1 ELSE 0 END) en_linkedin,
    ROUND(100.0*SUM(CASE WHEN en_linkedin THEN 1 ELSE 0 END)/COUNT(*),1) pct_linkedin,
    SUM(CASE WHEN monto_utm IS NOT NULL THEN 1 ELSE 0 END) con_monto,
    ROUND(SUM(monto_utm)) total_utm,
    ROUND(AVG(monto_utm),1) avg_utm,
    ROUND(MEDIAN(monto_utm),1) median_utm,
    SUM(CASE WHEN decil_avaluo >= 8 THEN 1 ELSE 0 END) patrimonio_alto,
    SUM(CASE WHEN total_vehiculos >= 1 THEN 1 ELSE 0 END) con_vehiculos,
    SUM(CASE WHEN total_propiedades >= 1 THEN 1 ELSE 0 END) con_propiedades,
    SUM(CASE WHEN en_pjud THEN 1 ELSE 0 END) con_pjud,
    SUM(CASE WHEN n_civil_ddo > 0 THEN 1 ELSE 0 END) con_civil_ddo
FROM full_t
""").fetchone()
out["resumen"] = {
    "total": r[0], "en_linkedin": r[1], "pct_linkedin": r[2],
    "con_monto": r[3], "total_utm": r[4], "avg_utm": r[5], "median_utm": r[6],
    "total_clp": int((r[4] or 0) * UTM_CLP), "total_usd": int((r[4] or 0) * UTM_USD),
    "utm_clp": UTM_CLP, "usd_clp": USD_CLP, "utm_usd": round(UTM_USD, 2),
    "fecha_conversion": "2026-04-17",
    "patrimonio_alto": r[7], "con_vehiculos": r[8], "con_propiedades": r[9],
    "con_pjud": r[10], "con_civil_ddo": r[11],
}

# ═══ Cortes globales (2026) ═══
out["por_region"] = table("SELECT COALESCE(region,'(sin región)') region, COUNT(*) n, ROUND(SUM(monto_utm)) utm_total FROM full_t GROUP BY 1 ORDER BY 2 DESC")
for row in out["por_region"]: row["macrozona"] = MACROZONAS.get(row["region"], "(sin macrozona)")
out["por_macrozona"] = {}
for row in out["por_region"]:
    mz = row["macrozona"]
    out["por_macrozona"].setdefault(mz, {"macrozona": mz, "n": 0, "utm_total": 0})
    out["por_macrozona"][mz]["n"] += row["n"]
    out["por_macrozona"][mz]["utm_total"] += row["utm_total"] or 0
out["por_macrozona"] = sorted(out["por_macrozona"].values(), key=lambda x: -x["n"])

out["por_comuna"] = table("SELECT COALESCE(comuna,'(sin comuna)') comuna, cod_comuna, COALESCE(region,'(sin región)') region, COUNT(*) n FROM full_t GROUP BY 1,2,3 ORDER BY 4 DESC")
out["por_sexo"] = table("SELECT COALESCE(sexo,'(sin dato)') sexo, COUNT(*) n FROM full_t GROUP BY 1 ORDER BY 2 DESC")
out["por_edad"] = table("""
SELECT CASE WHEN edad < 26 THEN '18-25' WHEN edad < 36 THEN '26-35' WHEN edad < 46 THEN '36-45'
    WHEN edad < 56 THEN '46-55' WHEN edad < 66 THEN '56-65' WHEN edad >= 66 THEN '66+' ELSE '(sin dato)' END rango_edad,
    COUNT(*) n FROM full_t GROUP BY 1
ORDER BY CASE rango_edad WHEN '18-25' THEN 1 WHEN '26-35' THEN 2 WHEN '36-45' THEN 3
    WHEN '46-55' THEN 4 WHEN '56-65' THEN 5 WHEN '66+' THEN 6 ELSE 7 END""")
out["por_edad_sexo"] = table("""
SELECT CASE WHEN edad < 26 THEN '18-25' WHEN edad < 36 THEN '26-35' WHEN edad < 46 THEN '36-45'
    WHEN edad < 56 THEN '46-55' WHEN edad < 66 THEN '56-65' WHEN edad >= 66 THEN '66+' ELSE '(sin dato)' END rango_edad,
    COALESCE(sexo,'(sin dato)') sexo, COUNT(*) n FROM full_t GROUP BY 1,2""")
out["por_decil"] = table("SELECT COALESCE(CAST(decil_avaluo AS VARCHAR),'(sin decil)') decil, COUNT(*) n FROM full_t GROUP BY 1 ORDER BY decil")
out["por_nse"] = table("SELECT COALESCE(nse,'(sin dato)') nse, COUNT(*) n FROM full_t GROUP BY 1 ORDER BY 2 DESC")
out["por_vehiculos"] = table("""SELECT CASE WHEN total_vehiculos IS NULL OR total_vehiculos=0 THEN '0'
    WHEN total_vehiculos=1 THEN '1' WHEN total_vehiculos=2 THEN '2'
    WHEN total_vehiculos<=4 THEN '3-4' ELSE '5+' END bucket, COUNT(*) n FROM full_t GROUP BY 1 ORDER BY bucket""")
out["por_propiedades"] = table("""SELECT CASE WHEN total_propiedades IS NULL OR total_propiedades=0 THEN '0'
    WHEN total_propiedades=1 THEN '1' WHEN total_propiedades=2 THEN '2'
    WHEN total_propiedades<=4 THEN '3-4' ELSE '5+' END bucket, COUNT(*) n FROM full_t GROUP BY 1 ORDER BY bucket""")
out["condicion_propietario"] = table("""SELECT CASE
    WHEN (total_propiedades IS NULL OR total_propiedades=0) THEN 'Arrendatario/Allegado'
    WHEN (total_vehiculos IS NULL OR total_vehiculos=0) THEN 'Solo propietario'
    ELSE 'Propietario con vehículos' END bucket, COUNT(*) n FROM full_t GROUP BY 1 ORDER BY 2 DESC""")
out["por_monto_bucket"] = table("""SELECT CASE WHEN monto_utm IS NULL THEN '(sin registro)'
    WHEN monto_utm < 50 THEN '0-50 UTM' WHEN monto_utm < 100 THEN '50-100 UTM'
    WHEN monto_utm < 200 THEN '100-200 UTM' WHEN monto_utm < 500 THEN '200-500 UTM'
    WHEN monto_utm < 1000 THEN '500-1.000 UTM' WHEN monto_utm < 2000 THEN '1.000-2.000 UTM'
    ELSE '2.000+ UTM' END bucket, COUNT(*) n, ROUND(AVG(monto_utm),1) utm_avg
FROM full_t GROUP BY 1
ORDER BY CASE bucket WHEN '0-50 UTM' THEN 1 WHEN '50-100 UTM' THEN 2 WHEN '100-200 UTM' THEN 3
    WHEN '200-500 UTM' THEN 4 WHEN '500-1.000 UTM' THEN 5 WHEN '1.000-2.000 UTM' THEN 6
    WHEN '2.000+ UTM' THEN 7 ELSE 8 END""")
out["por_universidad"] = table("""SELECT universidad_principal AS universidad,
    COUNT(*) n, ROUND(SUM(monto_utm)) utm_total, ROUND(AVG(monto_utm),1) utm_avg
FROM full_t WHERE universidad_principal IS NOT NULL GROUP BY 1 ORDER BY 2 DESC""")
out["por_seniority"] = table("SELECT COALESCE(seniority,'(sin dato)') seniority, COUNT(*) n FROM full_t WHERE en_linkedin GROUP BY 1 ORDER BY 2 DESC")
out["por_tier"] = table("SELECT COALESCE(tier,'(sin dato)') tier, COUNT(*) n FROM full_t WHERE en_linkedin GROUP BY 1 ORDER BY 2 DESC")
out["top_industrias"] = table("SELECT industry, COUNT(*) n FROM full_t WHERE en_linkedin AND industry IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 30")
out["top_empresas"] = table("""SELECT company, COUNT(*) n FROM full_t WHERE en_linkedin AND company IS NOT NULL
    AND company NOT IN ('autónomo','independiente','profesional independiente','colegio')
    GROUP BY 1 ORDER BY 2 DESC LIMIT 50""")

out["perfiles"] = table("""WITH p AS (SELECT CASE
    WHEN en_linkedin AND seniority IN ('c-level','director') THEN '1. Ejecutivo (C-Level/Director)'
    WHEN en_linkedin AND seniority = 'manager' THEN '2. Gerente'
    WHEN en_linkedin AND seniority = 'academic' THEN '3. Académico'
    WHEN en_linkedin AND seniority = 'senior' THEN '4. Profesional senior'
    WHEN en_linkedin AND seniority = 'professional' THEN '5. Profesional'
    WHEN en_linkedin AND seniority = 'operational' THEN '6. Operativo'
    WHEN en_linkedin THEN '7. En LinkedIn (sin clasificar)'
    WHEN decil_avaluo >= 8 OR total_propiedades >= 2 THEN '8. Alto patrimonio (sin LinkedIn)'
    WHEN total_vehiculos >= 1 OR total_propiedades >= 1 THEN '9. Con patrimonio (sin LinkedIn)'
    ELSE '10. Sin información adicional' END perfil FROM full_t)
SELECT perfil, COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),1) pct FROM p GROUP BY 1 ORDER BY 1""")

# ═══ EVOLUCIÓN 2022-2026 ═══
evol = con.execute("""
SELECT year, COUNT(DISTINCT rut_dv) deudores, COUNT(*) filas,
    ROUND(SUM(monto_utm)) utm_total, ROUND(AVG(monto_utm),1) utm_avg, ROUND(MEDIAN(monto_utm),1) utm_med
FROM nominas GROUP BY 1 ORDER BY 1
""").fetchdf().to_dict(orient="records")
for e in evol:
    e["clp_total"] = int(e["utm_total"] * UTM_CLP)
    e["usd_total"] = int(e["utm_total"] * UTM_USD)
out["evolucion"] = evol

# Evolución por universidad (top 10 + resto)
evol_univ = con.execute("""
SELECT year, universidad_canon, COUNT(DISTINCT rut_dv) deudores, ROUND(SUM(monto_utm)) utm_total
FROM nominas GROUP BY 1,2
""").fetchdf().to_dict(orient="records")
out["evolucion_universidad"] = evol_univ

# ═══ FLUJOS 2022→2023→2024→2025→2026 ═══
# Para cada par de años consecutivos, categorizamos el movimiento del deudor.
flujos_transiciones = []
for y0, y1 in [(2022,2023),(2023,2024),(2024,2025),(2025,2026)]:
    # pivot: rut -> (monto_y0, monto_y1)
    r = con.execute(f"""
    WITH a AS (SELECT rut_dv, monto_utm m0 FROM nominas WHERE year={y0}),
         b AS (SELECT rut_dv, monto_utm m1 FROM nominas WHERE year={y1})
    SELECT
        '{y0}→{y1}' transicion,
        SUM(CASE WHEN a.rut_dv IS NULL AND b.rut_dv IS NOT NULL THEN 1 ELSE 0 END) nuevo,
        SUM(CASE WHEN a.rut_dv IS NOT NULL AND b.rut_dv IS NULL THEN 1 ELSE 0 END) salio,
        SUM(CASE WHEN a.rut_dv IS NOT NULL AND b.rut_dv IS NOT NULL AND b.m1 < a.m0 - 1 THEN 1 ELSE 0 END) pago_parcial,
        SUM(CASE WHEN a.rut_dv IS NOT NULL AND b.rut_dv IS NOT NULL AND b.m1 > a.m0 + 1 THEN 1 ELSE 0 END) acumulo,
        SUM(CASE WHEN a.rut_dv IS NOT NULL AND b.rut_dv IS NOT NULL AND ABS(b.m1 - a.m0) <= 1 THEN 1 ELSE 0 END) estable,
        ROUND(SUM(CASE WHEN a.rut_dv IS NOT NULL AND b.rut_dv IS NULL THEN a.m0 ELSE 0 END)) utm_salio,
        ROUND(SUM(CASE WHEN a.rut_dv IS NOT NULL AND b.rut_dv IS NOT NULL AND b.m1 < a.m0 THEN a.m0 - b.m1 ELSE 0 END)) utm_pago_parcial,
        ROUND(SUM(CASE WHEN a.rut_dv IS NOT NULL AND b.rut_dv IS NOT NULL AND b.m1 > a.m0 THEN b.m1 - a.m0 ELSE 0 END)) utm_intereses
    FROM a FULL OUTER JOIN b ON a.rut_dv = b.rut_dv
    """).fetchone()
    flujos_transiciones.append({
        "transicion": r[0], "nuevos": r[1], "salieron": r[2],
        "pago_parcial": r[3], "acumulo_intereses": r[4], "estable": r[5],
        "utm_estimada_pagada_salio": r[6], "utm_estimada_pago_parcial": r[7], "utm_estimada_intereses": r[8],
    })
out["flujos"] = flujos_transiciones

# ═══ JUSTICIA ═══
out["justicia_resumen"] = {
    "total": out["resumen"]["total"],
    "con_pjud": out["resumen"]["con_pjud"],
    "con_civil_ddo": out["resumen"]["con_civil_ddo"],
    "pct_con_pjud": round(100 * out["resumen"]["con_pjud"] / out["resumen"]["total"], 1),
    "pct_con_civil_ddo": round(100 * out["resumen"]["con_civil_ddo"] / out["resumen"]["total"], 1),
}
out["justicia_por_causa"] = table("""SELECT CASE
    WHEN n_civil = 0 AND n_laboral = 0 THEN 'Sin causas'
    WHEN n_civil = 1 AND n_laboral = 0 THEN '1 causa civil'
    WHEN n_civil BETWEEN 2 AND 3 AND n_laboral = 0 THEN '2-3 civiles'
    WHEN n_civil BETWEEN 4 AND 10 THEN '4-10 civiles'
    WHEN n_civil > 10 THEN '10+ civiles'
    WHEN n_laboral > 0 THEN 'Con laboral' ELSE 'Otro' END bucket, COUNT(*) n FROM full_t GROUP BY 1""")
out["justicia_por_region"] = table("""SELECT COALESCE(region,'(sin región)') region,
    COUNT(*) n, SUM(CASE WHEN n_civil_ddo > 0 THEN 1 ELSE 0 END) demandados_civil,
    ROUND(100.0*SUM(CASE WHEN n_civil_ddo > 0 THEN 1 ELSE 0 END)/COUNT(*),1) pct_demandados
FROM full_t GROUP BY 1 ORDER BY 2 DESC""")
out["justicia_por_universidad"] = table("""SELECT universidad_principal universidad,
    COUNT(*) n, SUM(CASE WHEN n_civil_ddo > 0 THEN 1 ELSE 0 END) demandados_civil,
    ROUND(100.0*SUM(CASE WHEN n_civil_ddo > 0 THEN 1 ELSE 0 END)/COUNT(*),1) pct_demandados
FROM full_t WHERE universidad_principal IS NOT NULL GROUP BY 1 ORDER BY 2 DESC""")
out["justicia_por_perfil"] = table("""WITH p AS (SELECT n_civil_ddo, CASE
    WHEN en_linkedin AND seniority IN ('c-level','director') THEN 'Ejecutivo'
    WHEN en_linkedin AND seniority = 'manager' THEN 'Gerente'
    WHEN en_linkedin AND seniority = 'academic' THEN 'Académico'
    WHEN en_linkedin AND seniority = 'senior' THEN 'Profesional sr'
    WHEN en_linkedin AND seniority = 'professional' THEN 'Profesional'
    WHEN en_linkedin AND seniority = 'operational' THEN 'Operativo'
    WHEN en_linkedin THEN 'LK sin clasif'
    WHEN decil_avaluo >= 8 THEN 'Alto patrimonio'
    ELSE 'Sin info' END perfil FROM full_t)
SELECT perfil, COUNT(*) n, SUM(CASE WHEN n_civil_ddo > 0 THEN 1 ELSE 0 END) demandados,
    ROUND(100.0*SUM(CASE WHEN n_civil_ddo > 0 THEN 1 ELSE 0 END)/COUNT(*),1) pct
FROM p GROUP BY 1 ORDER BY 2 DESC""")

# ═══ XRAY UNIVERSIDAD ═══
# Para cada universidad canonica: resumen + evolución + demografía + justicia
univs = con.execute("SELECT DISTINCT universidad_canon FROM nominas WHERE year=2026 ORDER BY 1").fetchdf()['universidad_canon'].tolist()
xray = {}
for u in univs:
    row = {}
    # Resumen 2026
    r = con.execute(f"""
    SELECT COUNT(*) n, ROUND(SUM(monto_utm)) utm_total, ROUND(AVG(monto_utm),1) utm_avg, ROUND(MEDIAN(monto_utm),1) utm_med
    FROM nominas WHERE year=2026 AND universidad_canon = ?
    """, [u]).fetchone()
    if not r or r[0] < K: continue  # suprime universidades con menos de K
    row["resumen"] = {
        "universidad": u, "deudores_2026": r[0],
        "utm_total": r[1], "clp_total": int((r[1] or 0) * UTM_CLP), "usd_total": int((r[1] or 0) * UTM_USD),
        "utm_avg": r[2], "utm_median": r[3],
    }
    # Evolución
    ev = con.execute(f"""
    SELECT year, COUNT(DISTINCT rut_dv) deudores, ROUND(SUM(monto_utm)) utm_total, ROUND(AVG(monto_utm),1) utm_avg
    FROM nominas WHERE universidad_canon = ? GROUP BY year ORDER BY year
    """, [u]).fetchdf().to_dict(orient="records")
    row["evolucion"] = ev
    # Demografía (solo 2026, solo los del subconjunto enriched que tienen esta universidad)
    row["por_region"] = con.execute("""
    SELECT COALESCE(region,'(sin región)') region, COUNT(*) n
    FROM full_t WHERE universidad_principal = ? GROUP BY 1
    HAVING COUNT(*) >= ? ORDER BY 2 DESC
    """, [u, K]).fetchdf().to_dict(orient="records")
    row["por_edad"] = con.execute("""
    SELECT CASE WHEN edad < 26 THEN '18-25' WHEN edad < 36 THEN '26-35' WHEN edad < 46 THEN '36-45'
        WHEN edad < 56 THEN '46-55' WHEN edad < 66 THEN '56-65' WHEN edad >= 66 THEN '66+' ELSE '(sin dato)' END rango_edad,
        COUNT(*) n FROM full_t WHERE universidad_principal = ? GROUP BY 1 HAVING COUNT(*) >= ?
    ORDER BY CASE rango_edad WHEN '18-25' THEN 1 WHEN '26-35' THEN 2 WHEN '36-45' THEN 3
        WHEN '46-55' THEN 4 WHEN '56-65' THEN 5 WHEN '66+' THEN 6 ELSE 7 END
    """, [u, K]).fetchdf().to_dict(orient="records")
    row["por_sexo"] = con.execute("""
    SELECT COALESCE(sexo,'(sin dato)') sexo, COUNT(*) n FROM full_t
    WHERE universidad_principal = ? GROUP BY 1 HAVING COUNT(*) >= ? ORDER BY 2 DESC
    """, [u, K]).fetchdf().to_dict(orient="records")
    row["por_seniority"] = con.execute("""
    SELECT COALESCE(seniority,'(sin dato)') seniority, COUNT(*) n FROM full_t
    WHERE universidad_principal = ? AND en_linkedin GROUP BY 1 HAVING COUNT(*) >= ? ORDER BY 2 DESC
    """, [u, K]).fetchdf().to_dict(orient="records")
    row["por_monto_bucket"] = con.execute("""
    SELECT CASE WHEN monto_utm < 50 THEN '0-50 UTM' WHEN monto_utm < 100 THEN '50-100 UTM'
        WHEN monto_utm < 200 THEN '100-200 UTM' WHEN monto_utm < 500 THEN '200-500 UTM'
        WHEN monto_utm < 1000 THEN '500-1.000 UTM' WHEN monto_utm < 2000 THEN '1.000-2.000 UTM'
        ELSE '2.000+ UTM' END bucket, COUNT(*) n
    FROM nominas WHERE year=2026 AND universidad_canon = ? GROUP BY 1 HAVING COUNT(*) >= ?
    ORDER BY CASE bucket WHEN '0-50 UTM' THEN 1 WHEN '50-100 UTM' THEN 2 WHEN '100-200 UTM' THEN 3
        WHEN '200-500 UTM' THEN 4 WHEN '500-1.000 UTM' THEN 5 WHEN '1.000-2.000 UTM' THEN 6
        WHEN '2.000+ UTM' THEN 7 END
    """, [u, K]).fetchdf().to_dict(orient="records")
    # Justicia (solo si subset cumple k)
    pj = con.execute("""
    SELECT COUNT(*) n, SUM(CASE WHEN n_civil_ddo > 0 THEN 1 ELSE 0 END) demandados
    FROM full_t WHERE universidad_principal = ?
    """, [u]).fetchone()
    if pj and pj[0] >= K:
        row["justicia"] = {"total": pj[0], "demandados_civil": pj[1],
                           "pct_demandados": round(100*pj[1]/pj[0], 1) if pj[0] else 0}
    xray[u] = row
out["xray_universidades"] = xray

# ═══ WRITE ═══
with open(OUT, "w") as f:
    json.dump(out, f, ensure_ascii=False, indent=2, default=str)
print(f"\nEscrito {OUT} ({OUT.stat().st_size/1024:.1f} KB)")
print(f"Claves: {list(out.keys())}")
print(f"Universidades Xray: {len(xray)}")
for y in sorted([e['year'] for e in evol]):
    e = next(x for x in evol if x['year']==y)
    print(f"  {y}: {e['deudores']:,} deudores · {e['utm_total']/1e6:.1f}M UTM · ${e['clp_total']/1e12:.2f} bln CLP")
print("\nFlujos:")
for f in flujos_transiciones:
    print(f"  {f['transicion']}: nuevos={f['nuevos']:,} salieron={f['salieron']:,} pago_parcial={f['pago_parcial']:,} acumuló={f['acumulo_intereses']:,} estable={f['estable']:,}")
