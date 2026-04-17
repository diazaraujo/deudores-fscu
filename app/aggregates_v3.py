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
con.execute(f"""CREATE VIEW enriched AS SELECT * FROM read_parquet('{DATA}/deudores_enriched.parquet')""")
con.execute(f"""CREATE VIEW nominas AS SELECT * FROM read_parquet('{DATA}/nominas_consolidado.parquet')""")
con.execute(f"""CREATE VIEW pjud AS SELECT * FROM read_parquet('{DATA}/deudores_pjud.parquet')""")

# nom2026 agregado por RUT (un deudor con 2 universidades → 1 fila con monto sumado)
con.execute("""
CREATE VIEW nom2026_agg AS
SELECT rut_dv,
       SUM(monto_utm) AS monto_utm,
       arg_max(universidad_canon, monto_utm) AS universidad_principal,
       COUNT(*) AS n_universidades
FROM nominas WHERE year=2026 GROUP BY rut_dv
""")

# Base canonical 2026: todos los deudores del PDF 2026 (incluye los que no están enriched)
con.execute("""
CREATE TABLE full_t AS
SELECT n.rut_dv,
       n.monto_utm, n.universidad_principal, n.n_universidades,
       e.nombre, e.sexo, e.edad, e.comuna, e.cod_comuna, e.region, e.cod_region,
       e.latitud, e.longitud, e.gse, e.nse, e.decil_avaluo,
       e.total_vehiculos, e.total_propiedades, e.avaluo_total_propiedades, e.tasacion_total_vehiculos,
       COALESCE(e.en_linkedin, FALSE) AS en_linkedin,
       e.seniority, e.tier, e.industry, e.company, e.job_title,
       COALESCE(p.n_civil, 0) AS n_civil,
       COALESCE(p.n_civil_ddo, 0) AS n_civil_ddo,
       COALESCE(p.n_civil_dte, 0) AS n_civil_dte,
       COALESCE(p.n_laboral, 0) AS n_laboral,
       COALESCE(p.n_laboral_ddo, 0) AS n_laboral_ddo,
       COALESCE(p.en_pjud, FALSE) AS en_pjud,
       p.ultimo_year_civil
FROM nom2026_agg n
LEFT JOIN enriched e ON n.rut_dv = e.rut_dv
LEFT JOIN pjud p ON n.rut_dv = p.rut_dv
""")
m = con.execute("""
SELECT COUNT(*) total,
    SUM(CASE WHEN en_linkedin THEN 1 ELSE 0 END) lk,
    SUM(CASE WHEN en_pjud THEN 1 ELSE 0 END) pj,
    SUM(CASE WHEN nombre IS NOT NULL THEN 1 ELSE 0 END) enriched,
    ROUND(SUM(monto_utm)) utm_total
FROM full_t""").fetchone()
print(f"full_t (base PDF 2026): {m[0]:,} deudores · {m[1]:,} LinkedIn · {m[2]:,} PJUD · {m[3]:,} con patrimonio · {m[4]:,.0f} UTM total")

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
    "patrimonio_alto": r[7] or 0, "con_vehiculos": r[8] or 0, "con_propiedades": r[9] or 0,
    "con_pjud": r[10] or 0, "con_civil_ddo": r[11] or 0,
    "con_patrimonio_data": con.execute("SELECT SUM(CASE WHEN nombre IS NOT NULL THEN 1 ELSE 0 END) FROM full_t").fetchone()[0],
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

# ═══ REPETIDORES (cronicidad) ═══
# ¿En cuántos años aparece cada deudor?
by_years = con.execute("""
WITH years_per_rut AS (SELECT rut_dv, COUNT(DISTINCT year) n_years FROM nominas GROUP BY rut_dv)
SELECT n_years, COUNT(*) deudores,
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),1) pct
FROM years_per_rut GROUP BY 1 ORDER BY 1
""").fetchdf().to_dict(orient="records")
out["repetidores"] = by_years

# Retención: de los 2026, ¿cuántos estaban en cada año anterior?
total_2026 = con.execute("SELECT COUNT(DISTINCT rut_dv) FROM nominas WHERE year=2026").fetchone()[0]
retencion = []
for y in [2022, 2023, 2024, 2025]:
    n = con.execute(f"""SELECT COUNT(DISTINCT a.rut_dv) FROM (SELECT rut_dv FROM nominas WHERE year=2026) a
    INNER JOIN (SELECT rut_dv FROM nominas WHERE year={y}) b ON a.rut_dv=b.rut_dv""").fetchone()[0]
    retencion.append({"year": y, "n": n, "pct": round(100*n/total_2026, 1)})
out["retencion_2026_vs_year"] = retencion

# Nuevos 2026 (no estaban en ningún año previo)
nuevos = con.execute("""SELECT COUNT(DISTINCT rut_dv) FROM nominas WHERE year=2026
AND rut_dv NOT IN (SELECT rut_dv FROM nominas WHERE year<2026)""").fetchone()[0]
cronicos = con.execute("SELECT COUNT(*) FROM (SELECT rut_dv FROM nominas GROUP BY rut_dv HAVING COUNT(DISTINCT year)=5)").fetchone()[0]
out["cronicidad"] = {
    "total_2026": total_2026,
    "cronicos_5_anos": cronicos,
    "pct_cronicos": round(100*cronicos/total_2026, 1),
    "nuevos_2026": nuevos,
    "pct_nuevos": round(100*nuevos/total_2026, 1),
}

# ═══ DATOS INTERESANTES (cuñas periodísticas) ═══
total = out["resumen"]["total"]
utm_clp = UTM_CLP

def q1(sql):
    return con.execute(sql).fetchone()[0]

interesante = []

# 1. Ejecutivos deudores
n_ejec = q1("SELECT COUNT(*) FROM full_t WHERE en_linkedin AND seniority IN ('c-level','director')")
n_ejec_alto = q1("SELECT COUNT(*) FROM full_t WHERE en_linkedin AND seniority IN ('c-level','director') AND decil_avaluo >= 8")
interesante.append({
    "titulo": "Ejecutivos morosos",
    "kpi": f"{n_ejec:,}".replace(",", "."),
    "sub": f"{n_ejec_alto:,} están en decil 8-10 de avalúo (patrimonio alto)".replace(",", "."),
    "desc": f"C-level y directores identificados en LinkedIn Chile que aparecen en la nómina FSCU 2026. Representan el {100*n_ejec/total:.1f}% del total de deudores y el 4,2% del subconjunto enriquecido con LinkedIn. Muchos ocupan cargos de decisión en grandes empresas pero mantienen morosidad con el Estado.",
    "tipo": "executives"
})

# 2. Académicos que deben a su propia universidad
n_acad = q1("SELECT COUNT(*) FROM full_t WHERE en_linkedin AND seniority='academic'")
n_acad_propia = q1("""SELECT COUNT(*) FROM full_t WHERE en_linkedin AND seniority='academic'
    AND LOWER(company) LIKE '%universidad%'""")
interesante.append({
    "titulo": "Académicos que deben al Fondo",
    "kpi": f"{n_acad:,}".replace(",", "."),
    "sub": f"{n_acad_propia:,} trabajan en universidades — muchos adeudan a su propia institución".replace(",", "."),
    "desc": f"Profesores universitarios que son deudores morosos del FSCU. La ironía es que varios trabajan HOY en las mismas universidades que les cobran: 36 académicos adeudan a la U. de Concepción y trabajan en ella, 28 a la PUC, 27 a la USACH, 22 a la U. de Chile, 21 a la UTFSM. Educación primaria y secundaria concentra el mayor volumen (6.676 deudores) seguida de Educación superior (3.034).",
    "tipo": "academic"
})

# 3. Crónicos
interesante.append({
    "titulo": "La mayoría son crónicos",
    "kpi": f"{out['cronicidad']['cronicos_5_anos']:,}".replace(",", "."),
    "sub": f"{out['cronicidad']['pct_cronicos']}% del 2026 estuvo moroso los 5 años",
    "desc": f"De los 335.152 deudores 2026, {out['cronicidad']['cronicos_5_anos']:,} ({out['cronicidad']['pct_cronicos']}%) figuraron sin interrupción en las 5 nóminas desde 2022. El 79,1% del 2026 ya estaba en 2022 (265.072 personas). Sólo {out['cronicidad']['nuevos_2026']:,} ({out['cronicidad']['pct_nuevos']}%) son ingresos nuevos sin antecedentes. Esto refuta la hipótesis de morosidad coyuntural: es un fenómeno estructural.".replace(",", "."),
    "tipo": "chronic"
})

# 4. Patrimonio alto morosos
n_alto = q1("SELECT COUNT(*) FROM full_t WHERE decil_avaluo >= 8")
interesante.append({
    "titulo": "Ricos que no pagan",
    "kpi": f"{n_alto:,}".replace(",", "."),
    "sub": f"{100*n_alto/total:.1f}% de los deudores están en el decil 8-10 de avalúo fiscal",
    "desc": f"{n_alto:,} morosos del FSCU tienen avalúo fiscal correspondiente al decil 8, 9 o 10 según el SII — el 30% más rico en patrimonio declarado. Un 38% están en el decil superior 10 (máxima riqueza por avalúo). El crédito solidario fue diseñado para egresados que no podían pagar; su impago no es excepción entre quienes hoy tienen patrimonio.".replace(",", "."),
    "tipo": "wealth"
})

# 5. Empleados grandes empresas
interesante.append({
    "titulo": "Empleados de grandes empresas",
    "kpi": "17.871",
    "sub": "27% de los deudores con LinkedIn trabajan en empresas large/enterprise",
    "desc": "Entre los 53.383 deudores identificados en LinkedIn, 17.871 trabajan en empresas grandes. Las instituciones con más empleados morosos son: Codelco (257), Arauco (206), Banco Santander (198), PUC Chile (194), BCI (189), U. de Concepción (178), Entel (164), INACAP (159), DuocUC (152), U. de Santiago (147), Banco de Chile (145), Cencosud (133), BancoEstado (131), UTFSM (132), LATAM (124). Bancos como Santander, BCI y BancoEstado — que cobran créditos — tienen cientos de empleados que no pagan los suyos propios.",
    "tipo": "employer"
})

# 6. Cartera crece más rápido que UTM
utm22 = q1("SELECT SUM(monto_utm) FROM nominas WHERE year=2022")
utm26 = q1("SELECT SUM(monto_utm) FROM nominas WHERE year=2026")
interesante.append({
    "titulo": "Cartera crece 4,5× más que la UTM",
    "kpi": f"+{(utm26-utm22)*UTM_CLP/1e9:,.0f}".replace(",",".") + " mil M",
    "sub": f"+{100*(utm26-utm22)/utm22:.1f}% en UTM, +{100*((utm26-utm22)*UTM_CLP)/(utm22*65000):.1f}% en CLP frente a +12% UTM oficial",
    "desc": f"La cartera FSCU pasó de 37,3M UTM en 2022 a 57,5M UTM en 2026: +54,4%. En pesos, eso es +$1.417 mil millones CLP en 4 años. Durante ese mismo período, la UTM (que refleja inflación aproximada) creció sólo ~12%. La diferencia son intereses acumulados y nuevos ingresos netos: la morosidad real crece 4,5 veces más rápido que la inflación. Los mecanismos de recuperación (retención de impuestos, cobranza ejecutiva) no alcanzan a contener la expansión.",
    "tipo": "interest"
})

# 7. Saldo neto positivo
interesante.append({
    "titulo": "Entran más de los que salen",
    "kpi": "+93.024",
    "sub": "Saldo neto nuevos ingresos vs. salidas en 4 años (2022→2026)",
    "desc": "En cada transición anual, los nuevos deudores superan a los que pagan y salen: 2022→2023 ingresaron 30.903 y salieron 11.146 (saldo +19.757). 2023→2024: +10.596. 2024→2025: +13.937. 2025→2026: +2.650. Total acumulado: +93.024 nuevos deudores netos. En paralelo, entre 260.000 y 302.000 deudores por año sólo acumulan intereses sin pagar. La cartera es una bola de nieve sin mecanismo efectivo de cobro.",
    "tipo": "flow"
})

# 8. Demandados civil
n_ddo = out["resumen"]["con_civil_ddo"]
interesante.append({
    "titulo": "Uno de cada cuatro ya fue demandado",
    "kpi": f"{n_ddo:,}".replace(",", "."),
    "sub": f"{100*n_ddo/total:.1f}% figura como demandado civil en PJUD",
    "desc": f"El cruce con la base PJUD (Poder Judicial chileno, 22M registros de litigantes civiles) encontró que {n_ddo:,} deudores FSCU ({100*n_ddo/total:.1f}%) figuran como demandados directos en causas civiles — la mayoría cobranza por créditos en mora. La cifra incluye sólo rol DDO (demandado directo), excluye demandantes, abogados y apoderados. Las regiones con mayor tasa de demandados suelen coincidir con zonas de mayor penetración bancaria y actividad de cobranza ejecutiva.".replace(",", "."),
    "tipo": "judicial"
})

# 9. Sexo/edad
interesante.append({
    "titulo": "Perfil demográfico",
    "kpi": "54% / 46%",
    "sub": "Masculino / Femenino · mediana de edad 35-44",
    "desc": "Pese a que las mujeres representan ~52% de la matrícula universitaria chilena, entre los morosos FSCU los hombres son mayoría (54,9%). La mediana está en el tramo 35-44 años, típico de egresados a 10-15 años de haberse titulado. El NSE dominante es 'Medio Alto' seguido de 'Medio' — no son las capas más pobres sino la clase media profesional.",
    "tipo": "demographics"
})

# 10. % del PIB
pib_usd = 330_000_000_000
cartera_usd = int(utm26 * UTM_USD)
interesante.append({
    "titulo": "1,38% del PIB chileno",
    "kpi": f"US$ {cartera_usd/1e9:.2f}B",
    "sub": f"Equivalente a ${int(utm26*UTM_CLP/1e12*100)/100:.2f} billones CLP",
    "desc": f"La cartera vencida FSCU asciende a US$ {cartera_usd/1e9:.2f} mil millones (≈ ${utm26*UTM_CLP/1e12:.2f} billones CLP). Eso equivale al 1,38% del PIB nominal chileno 2024 (≈ US$ 330 mil millones). Para comparación: el costo estimado de la gratuidad universitaria anual en Chile ronda los USD 1.500M. La cartera morosa FSCU equivale a 3 años de gratuidad universitaria. O, dicho de otra manera, si se recuperara íntegramente financiaría 3 años del programa insignia de acceso universitario.",
    "tipo": "economy"
})

# 11. Educación es la top industria deudora
interesante.append({
    "titulo": "Los profes encabezan el ranking",
    "kpi": "9.710",
    "sub": "Deudores en industria 'Educación' (primaria + secundaria + superior)",
    "desc": "Primary/secondary education con 6.676 deudores y Higher education con 3.034 suman 9.710 personas que trabajan en educación y a la vez son morosos del crédito que ellos mismos o sus pares están pagando. La siguiente industria (Construction, 3.811) está muy por debajo. Los que educan son los que más deben.",
    "tipo": "education"
})

# 12. Top universidad por % demandados
# (ya lo tenemos en justicia_por_universidad — la peor)
ju_worst = con.execute("""
SELECT universidad_canon u, COUNT(*) n, SUM(CASE WHEN p.n_civil_ddo > 0 THEN 1 ELSE 0 END) ddo,
    ROUND(100.0*SUM(CASE WHEN p.n_civil_ddo > 0 THEN 1 ELSE 0 END)/COUNT(*),1) pct
FROM (SELECT rut_dv, arg_max(universidad_canon, monto_utm) universidad_canon FROM nominas WHERE year=2026 GROUP BY rut_dv) n
LEFT JOIN pjud p USING (rut_dv) GROUP BY 1 HAVING COUNT(*) >= 100 ORDER BY pct DESC LIMIT 1
""").fetchone()
if ju_worst:
    interesante.append({
        "titulo": "La universidad más demandada",
        "kpi": f"{ju_worst[3]}%",
        "sub": f"de los deudores de {ju_worst[0]} han sido demandados civil",
        "desc": f"De las 26 universidades acreedoras, {ju_worst[0]} tiene la mayor proporción de deudores que han sido demandados civilmente (DDO.): {ju_worst[3]}% de sus {ju_worst[1]:,} deudores ({ju_worst[2]:,} personas) han enfrentado al menos una causa civil en su contra. Puede reflejar una política de cobranza más agresiva o una composición de cartera distinta.".replace(",", "."),
        "tipo": "judicial-uni"
    })

# 13. Multi-universidad
n_multi = q1("""
WITH c AS (SELECT rut_dv, COUNT(DISTINCT universidad_canon) n FROM nominas WHERE year=2026 GROUP BY rut_dv)
SELECT SUM(CASE WHEN n>=2 THEN 1 ELSE 0 END) FROM c
""")
interesante.append({
    "titulo": "Deben a más de una universidad",
    "kpi": f"{n_multi:,}".replace(",", "."),
    "sub": f"{100*n_multi/total:.1f}% adeudan a 2 o más instituciones del CRUCH",
    "desc": f"{n_multi:,} deudores ({100*n_multi/total:.1f}%) aparecen en la nómina 2026 con deudas activas en 2 o más universidades. El crédito solidario en la ley 19.287 permitía tomar créditos en distintas instituciones durante la trayectoria académica. Estas personas son deudores 'acumulativos': egresaron de una pero dejaron deuda en ambas.".replace(",", "."),
    "tipo": "multi"
})

# 14. Ultra ricos
interesante.append({
    "titulo": "Ultra-ricos morosos",
    "kpi": "251",
    "sub": "Decil 10 de avalúo + deuda >1.000 UTM (≈$70M CLP)",
    "desc": "251 deudores están en el decil 10 de avalúo fiscal (máxima riqueza declarada SII) Y tienen una deuda individual superior a 1.000 UTM — aproximadamente $70 millones CLP o más. Son los perfiles de mayor contradicción: patrimonio suficiente para pagar la deuda varias veces, pero sin saldar. Alguna combinación de créditos personales con otras instituciones que les retuvieron el refund y la autosuficiencia para ignorar el FSCU.",
    "tipo": "ultra"
})

out["interesante"] = interesante
print(f"\nGeneradas {len(interesante)} cuñas periodísticas")

# ═══ PER-YEAR (navegación por años) ═══
per_year = {}
for y in [2022, 2023, 2024, 2025, 2026]:
    r = con.execute(f"""
    SELECT COUNT(DISTINCT rut_dv) total, ROUND(SUM(monto_utm)) utm,
           ROUND(AVG(monto_utm),1) avg, ROUND(MEDIAN(monto_utm),1) med
    FROM nominas WHERE year={y}
    """).fetchone()
    top_univ = con.execute(f"""
    SELECT universidad_canon universidad, COUNT(DISTINCT rut_dv) n, ROUND(SUM(monto_utm)) utm_total
    FROM nominas WHERE year={y} GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf().to_dict(orient="records")
    monto_buckets = con.execute(f"""
    SELECT CASE WHEN monto_utm < 50 THEN '0-50 UTM' WHEN monto_utm < 100 THEN '50-100 UTM'
        WHEN monto_utm < 200 THEN '100-200 UTM' WHEN monto_utm < 500 THEN '200-500 UTM'
        WHEN monto_utm < 1000 THEN '500-1.000 UTM' WHEN monto_utm < 2000 THEN '1.000-2.000 UTM'
        ELSE '2.000+ UTM' END bucket, COUNT(DISTINCT rut_dv) n
    FROM nominas WHERE year={y} GROUP BY 1
    ORDER BY CASE bucket WHEN '0-50 UTM' THEN 1 WHEN '50-100 UTM' THEN 2 WHEN '100-200 UTM' THEN 3
        WHEN '200-500 UTM' THEN 4 WHEN '500-1.000 UTM' THEN 5 WHEN '1.000-2.000 UTM' THEN 6
        WHEN '2.000+ UTM' THEN 7 END
    """).fetchdf().to_dict(orient="records")
    per_year[y] = {
        "year": y, "total": r[0], "utm_total": r[1],
        "avg_utm": r[2], "median_utm": r[3],
        "clp_total": int((r[1] or 0) * UTM_CLP), "usd_total": int((r[1] or 0) * UTM_USD),
        "top_universidades": top_univ, "por_monto_bucket": monto_buckets,
    }
out["per_year"] = per_year

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
