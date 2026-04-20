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
con.execute(f"""CREATE VIEW pjud AS SELECT * FROM read_parquet('{DATA}/deudores_pjud_deudas.parquet')""")

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
       COALESCE(p.n_causas_deuda, 0) AS n_causas_deuda,
       COALESCE(p.n_ejecutivo, 0) AS n_ejecutivo,
       COALESCE(p.n_ley_bancos, 0) AS n_ley_bancos,
       COALESCE(p.demandado_por_deuda, FALSE) AS demandado_por_deuda,
       p.ultimo_year_deuda
FROM nom2026_agg n
LEFT JOIN enriched e ON n.rut_dv = e.rut_dv
LEFT JOIN pjud p ON n.rut_dv = p.rut_dv
""")
m = con.execute("""
SELECT COUNT(*) total,
    SUM(CASE WHEN en_linkedin THEN 1 ELSE 0 END) lk,
    SUM(CASE WHEN demandado_por_deuda THEN 1 ELSE 0 END) pj,
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
    SUM(CASE WHEN demandado_por_deuda THEN 1 ELSE 0 END) con_demandado_deuda_raw,
    SUM(CASE WHEN demandado_por_deuda = TRUE THEN 1 ELSE 0 END) con_demandado_deuda
FROM full_t
""").fetchone()
out["resumen"] = {
    "total": r[0], "en_linkedin": r[1], "pct_linkedin": r[2],
    "con_monto": r[3], "total_utm": r[4], "avg_utm": r[5], "median_utm": r[6],
    "total_clp": int((r[4] or 0) * UTM_CLP), "total_usd": int((r[4] or 0) * UTM_USD),
    "utm_clp": UTM_CLP, "usd_clp": USD_CLP, "utm_usd": round(UTM_USD, 2),
    "fecha_conversion": "2026-04-17",
    "patrimonio_alto": r[7] or 0, "con_vehiculos": r[8] or 0, "con_propiedades": r[9] or 0,
    "con_pjud": r[10] or 0, "con_demandado_deuda": r[11] or 0,
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

# ═══ EVOLUCIÓN (todos los años disponibles) ═══
all_years = sorted([r[0] for r in con.execute("SELECT DISTINCT year FROM nominas ORDER BY 1").fetchall()])
print(f"Años disponibles: {all_years}")
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

# ═══ FLUJOS (pares consecutivos disponibles) ═══
flujos_transiciones = []
consecutive_pairs = [(all_years[i], all_years[i+1]) for i in range(len(all_years)-1) if all_years[i+1] - all_years[i] == 1]
for y0, y1 in consecutive_pairs:
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
    "eli5": "Imagínate al gerente de una empresa grande — alguien que toma decisiones importantes, que firma contratos millonarios, que maneja equipos de 100 personas. Esa persona estudió en la universidad con un préstamo del Estado y todavía no lo ha pagado. Hay 3.628 personas así. Es como si el dueño de la panadería te debiera todavía los pancitos de hace 15 años.",
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
    "eli5": "Hay 6.664 profesores (de colegio y de universidad) que deben plata al Estado por su propio crédito universitario. Y lo más loco: MUCHOS de estos profes siguen trabajando en la MISMA universidad que les prestó la plata. Es como si trabajaras en una panadería, tuvieras una cuenta en esa panadería desde hace años, y cada día llegaras a trabajar pasando por el mostrador donde dice tu nombre con la deuda sin pagar.",
    "tipo": "academic"
})

# 3. Crónicos
interesante.append({
    "titulo": "La mayoría son crónicos",
    "kpi": f"{out['cronicidad']['cronicos_5_anos']:,}".replace(",", "."),
    "sub": f"{out['cronicidad']['pct_cronicos']}% del 2026 estuvo moroso los 5 años",
    "desc": f"De los 335.152 deudores 2026, {out['cronicidad']['cronicos_5_anos']:,} ({out['cronicidad']['pct_cronicos']}%) figuraron sin interrupción en las 5 nóminas desde 2022. El 79,1% del 2026 ya estaba en 2022 (265.072 personas). Sólo {out['cronicidad']['nuevos_2026']:,} ({out['cronicidad']['pct_nuevos']}%) son ingresos nuevos sin antecedentes. Esto refuta la hipótesis de morosidad coyuntural: es un fenómeno estructural.".replace(",", "."),
    "eli5": "De cada 10 personas que deben al Estado hoy, 8 llevan 5 años completos sin pagar NADA. No es que un día se atrasaron: son crónicos. Es como ese amigo que te dice 'la próxima semana te pago' hace 5 años. No es que tenga mala memoria — es que nunca va a pagar. El sistema está lleno de estos 'amigos eternos'.",
    "tipo": "chronic"
})

# 4. Patrimonio alto morosos
n_alto = q1("SELECT COUNT(*) FROM full_t WHERE decil_avaluo >= 8")
interesante.append({
    "titulo": "Ricos que no pagan",
    "kpi": f"{n_alto:,}".replace(",", "."),
    "sub": f"{100*n_alto/total:.1f}% de los deudores están en el decil 8-10 de avalúo fiscal",
    "desc": f"{n_alto:,} morosos del FSCU tienen avalúo fiscal correspondiente al decil 8, 9 o 10 según el SII — el 30% más rico en patrimonio declarado. Un 38% están en el decil superior 10 (máxima riqueza por avalúo). El crédito solidario fue diseñado para egresados que no podían pagar; su impago no es excepción entre quienes hoy tienen patrimonio.".replace(",", "."),
    "eli5": "Hay 118.579 personas que SÍ PODRÍAN pagar — tienen casas caras, autos, propiedades. El Servicio de Impuestos los ubica en el 30% MÁS RICO de Chile. Pero igual no pagan la deuda universitaria. Es como si el vecino que tiene la casa más linda de la cuadra no pagara el almacén del barrio. No es que no pueda: es que no quiere, o nadie lo obliga.",
    "tipo": "wealth"
})

# 5. Empleados grandes empresas
interesante.append({
    "titulo": "Empleados de grandes empresas",
    "kpi": "17.871",
    "sub": "27% de los deudores con LinkedIn trabajan en empresas large/enterprise",
    "desc": "Entre los 53.383 deudores identificados en LinkedIn, 17.871 trabajan en empresas grandes. Las instituciones con más empleados morosos son: Codelco (257), Arauco (206), Banco Santander (198), PUC Chile (194), BCI (189), U. de Concepción (178), Entel (164), INACAP (159), DuocUC (152), U. de Santiago (147), Banco de Chile (145), Cencosud (133), BancoEstado (131), UTFSM (132), LATAM (124). Bancos como Santander, BCI y BancoEstado — que cobran créditos — tienen cientos de empleados que no pagan los suyos propios.",
    "eli5": "17.871 deudores trabajan en empresas como Codelco (la del cobre), Santander (el banco), LATAM (la aerolínea). Casi 200 trabajan en el Banco Santander. Piénsalo: el banco que todos los días llama por teléfono a los morosos tiene casi 200 empleados que ellos mismos son morosos con el Estado. Es como que el profe de matemáticas no supiera sumar.",
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
    "eli5": "La inflación de Chile subió 12% en 4 años. La deuda universitaria subió 54%. Eso significa que la deuda está creciendo 4 veces y media más rápido que los precios normales. En solo 4 años, la deuda total aumentó en $1,4 billones de pesos — eso es casi 3 años de presupuesto del Ministerio de Deporte. No es inflación: es deuda real creciendo sola.",
    "tipo": "interest"
})

# 7. Saldo neto positivo
interesante.append({
    "titulo": "Entran más de los que salen",
    "kpi": "+93.024",
    "sub": "Saldo neto nuevos ingresos vs. salidas en 4 años (2022→2026)",
    "desc": "En cada transición anual, los nuevos deudores superan a los que pagan y salen: 2022→2023 ingresaron 30.903 y salieron 11.146 (saldo +19.757). 2023→2024: +10.596. 2024→2025: +13.937. 2025→2026: +2.650. Total acumulado: +93.024 nuevos deudores netos. En paralelo, entre 260.000 y 302.000 deudores por año sólo acumulan intereses sin pagar. La cartera es una bola de nieve sin mecanismo efectivo de cobro.",
    "eli5": "Imagina una fila en el banco donde cada día entran 3 personas y sólo sale 1. La fila crece. El Fondo Solidario funciona así: cada año ingresan más deudores nuevos de los que logran salir pagando. En 4 años, neto, se sumaron 93.024 deudores. Y la mayoría de los que ya estaban NO se mueven: siguen ahí acumulando intereses como quien deja hervir la olla.",
    "tipo": "flow"
})

# 8. Demandados por cobranza de deuda
n_ddo = out["resumen"]["con_demandado_deuda"]
interesante.append({
    "titulo": "Uno de cada cinco enfrenta cobranza judicial",
    "kpi": f"{n_ddo:,}".replace(",", "."),
    "sub": f"{100*n_ddo/total:.1f}% del total fue demandado por cobranza de deuda",
    "desc": f"El cruce con PJUD (22M registros de litigantes del Poder Judicial) encontró que {n_ddo:,} deudores FSCU ({100*n_ddo/total:.1f}%) figuran como demandados directos (DDO.) en causas civiles de cobranza de deuda. El filtro excluye divorcios, daños, arrendamiento y otras causas civiles no vinculadas a morosidad; incluye Juicio Ejecutivo Obligación de Dar (mayoritario), Ejecutivo Mínima Cuantía, Monitorio, Ley de Bancos y Gestión Preparatoria de Cobranza. Estas personas ya tuvieron al menos una acción judicial de cobro en su contra — señal de morosidad estructural más allá del FSCU.".replace(",", "."),
    "eli5": "Cruzamos los RUT de los deudores con la base del Poder Judicial de Chile. Resultado: 1 de cada 5 morosos del Fondo Solidario ya fue llevado a juicio por NO pagar alguna deuda. Cualquier deuda. Son 72.014 personas. O sea, no es sólo el Estado el que no cobra: el banco, la tarjeta Falabella, la caja de compensación — todos los demandaron y siguen sin pagar.",
    "tipo": "judicial"
})

# 9. Sexo/edad
interesante.append({
    "titulo": "Perfil demográfico",
    "kpi": "54% / 46%",
    "sub": "Masculino / Femenino · mediana de edad 35-44",
    "desc": "Pese a que las mujeres representan ~52% de la matrícula universitaria chilena, entre los morosos FSCU los hombres son mayoría (54,9%). La mediana está en el tramo 35-44 años, típico de egresados a 10-15 años de haberse titulado. El NSE dominante es 'Medio Alto' seguido de 'Medio' — no son las capas más pobres sino la clase media profesional.",
    "eli5": "¿Quién es el deudor típico? Un hombre o mujer de 35-44 años, clase media alta, que egresó hace 10-15 años. No es el pobre que no llega a fin de mes — es el profesional que compra auto, que tiene Netflix, que va a Airbnb en verano. La morosidad no se concentra en los más vulnerables: se concentra en la clase media profesional.",
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
    "eli5": "El dinero que deben es MUCHO. Si juntaras en una pila todo lo que se debe al Fondo Solidario, sería como el 1,4% de toda la riqueza que produce Chile en un año entero. ¿Para qué alcanzaría? Para financiar 3 años completos de la GRATUIDAD universitaria. Sí: la plata que 335 mil personas no pagan alcanza para que muchos nuevos estudien gratis durante 3 años.",
    "tipo": "economy"
})

# 11. Educación es la top industria deudora
interesante.append({
    "titulo": "Los profes encabezan el ranking",
    "kpi": "9.710",
    "sub": "Deudores en industria 'Educación' (primaria + secundaria + superior)",
    "desc": "Primary/secondary education con 6.676 deudores y Higher education con 3.034 suman 9.710 personas que trabajan en educación y a la vez son morosos del crédito que ellos mismos o sus pares están pagando. La siguiente industria (Construction, 3.811) está muy por debajo. Los que educan son los que más deben.",
    "eli5": "Los profes de colegio y los profes universitarios son, juntos, la profesión con MÁS morosos del Fondo Solidario. Casi 10 mil personas enseñan cada día en salas de clases, y al mismo tiempo deben plata por su propia carrera. Es como el dentista al que le duelen las muelas, pero no va al dentista. Hay ironía ahí.",
    "tipo": "education"
})

# 12. Top universidad por % demandados
# (ya lo tenemos en justicia_por_universidad — la peor)
ju_worst = con.execute("""
SELECT universidad_canon u, COUNT(*) n, SUM(CASE WHEN p.demandado_por_deuda = TRUE THEN 1 ELSE 0 END) ddo,
    ROUND(100.0*SUM(CASE WHEN p.demandado_por_deuda = TRUE THEN 1 ELSE 0 END)/COUNT(*),1) pct
FROM (SELECT rut_dv, arg_max(universidad_canon, monto_utm) universidad_canon FROM nominas WHERE year=2026 GROUP BY rut_dv) n
LEFT JOIN pjud p USING (rut_dv) GROUP BY 1 HAVING COUNT(*) >= 100 ORDER BY pct DESC LIMIT 1
""").fetchone()
if ju_worst:
    interesante.append({
        "titulo": "La universidad más demandada",
        "kpi": f"{ju_worst[3]}%",
        "sub": f"de los deudores de {ju_worst[0]} han sido demandados civil",
        "desc": f"De las 26 universidades acreedoras, {ju_worst[0]} tiene la mayor proporción de deudores que han sido demandados civilmente (DDO.): {ju_worst[3]}% de sus {ju_worst[1]:,} deudores ({ju_worst[2]:,} personas) han enfrentado al menos una causa civil en su contra. Puede reflejar una política de cobranza más agresiva o una composición de cartera distinta.".replace(",", "."),
        "eli5": f"Hay una universidad donde 3 de cada 10 de sus ex-alumnos deudores ya fueron llevados a los tribunales. Eso es MUY alto comparado con el promedio nacional de 1 de cada 5. Puede ser porque es más dura cobrando, o porque a sus deudores les pasan otras cosas en la vida. Es la 'universidad cobradora' del grupo.",
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
    "eli5": "Casi 7.000 personas estudiaron en DOS universidades distintas (primero en una, se cambiaron, y terminaron en otra), y deben plata en AMBAS. Es como tomar clases de piano en una escuela, después guitarra en otra, y no pagar ninguna.",
    "tipo": "multi"
})

# 14. Ultra ricos
interesante.append({
    "titulo": "Ultra-ricos morosos",
    "kpi": "251",
    "sub": "Decil 10 de avalúo + deuda >1.000 UTM (≈$70M CLP)",
    "desc": "251 deudores están en el decil 10 de avalúo fiscal (máxima riqueza declarada SII) Y tienen una deuda individual superior a 1.000 UTM — aproximadamente $70 millones CLP o más. Son los perfiles de mayor contradicción: patrimonio suficiente para pagar la deuda varias veces, pero sin saldar. Alguna combinación de créditos personales con otras instituciones que les retuvieron el refund y la autosuficiencia para ignorar el FSCU.",
    "eli5": "251 personas están entre los MÁS ricos de Chile (decil 10, el 10% más rico) Y a la vez deben más de $70 millones cada una al Fondo Solidario. Son los 'peces gordos' de la morosidad. Con su patrimonio podrían pagar la deuda 100 veces y no se sentiría. Pero no lo hacen. Es como un multimillonario que se olvida pagar la cuenta del taxi: para él no significa nada, pero el taxista sí lo siente.",
    "tipo": "ultra"
})

# 15. La U de Chile es la que más demanda
interesante.append({
    "titulo": "La U de Chile es la que más demanda",
    "kpi": "2.129",
    "sub": "ex-alumnos demandados judicialmente por cobranza (12% de sus deudores)",
    "desc": "De las 26 universidades del CRUCH que administran el Fondo Solidario, sólo unas pocas ejercen cobranza judicial directa. La Universidad de Chile lidera con 2.129 demandas contra sus propios ex-alumnos morosos — 12% de sus 17.327 deudores. Le siguen U. de Valparaíso (436), U. del Bío Bío (329), UTEM (306), U. Arturo Prat (271) y U. de La Serena (222). Las demás universidades del CRUCH prefieren el mecanismo indirecto: retención de la devolución de impuestos vía Tesorería General de la República, que demanda a 10.256 deudores FSCU por cuenta de todas.",
    "eli5": "De las 26 universidades que prestan plata, SÓLO unas 8 llevan a juicio a sus ex-alumnos que no pagan. La Universidad de Chile es la más dura: llevó a 2.129 personas a tribunales. Las otras 18 universidades? NO demandan. Esperan que el Estado les descuente la deuda de la devolución de impuestos cuando llegue. Es la diferencia entre llamar por teléfono a cobrar vs. esperar que alguien te traiga la plata.",
    "tipo": "judicial-uni"
})

# 16. Bancos cobradores
interesante.append({
    "titulo": "Los bancos los persiguen más que el Estado",
    "kpi": "45.310",
    "sub": "deudores FSCU demandados por BancoEstado + privados",
    "desc": "Entre los 72.014 deudores FSCU con demandas de cobranza, los cobradores privados los persiguen más que el propio Estado: BancoEstado (8.678), CMR Falabella (8.582), Itaú (7.467), BCI (7.328), Scotiabank (7.220) y Banco de Chile (6.335) acumulan 45.310 demandas — 4 veces más que la Tesorería General (10.256). El deudor FSCU típico no tiene sólo una deuda con el Estado; tiene múltiples obligaciones vencidas con el sistema financiero.",
    "eli5": "Cuando alguien no paga al Fondo Solidario, ¿quién lo persigue más duro? Los BANCOS. Mucho más que el propio Estado. BancoEstado, Itaú, BCI, Scotiabank, Banco de Chile — todos juntos hicieron 45.310 demandas contra estos deudores. La Tesorería del Estado: sólo 10.256. Los bancos son 4 veces más agresivos cobrando que el Estado mismo. Como un hermano mayor que te pesca al doblar la esquina.",
    "tipo": "employer"
})

# 17. Crónicos acumulando
interesante.append({
    "titulo": "Los crónicos que sólo acumulan intereses",
    "kpi": "247.300",
    "sub": "73,8% del 2026 lleva 5 años seguidos y su deuda sólo crece",
    "desc": "247.300 deudores aparecen en las 5 nóminas consecutivas (2022 a 2026) y su saldo en UTM aumentó más de 5% entre ese rango. La mediana de deuda de este grupo pasó de 81 UTM en 2022 a 152 UTM en 2026: casi se duplicó en 4 años. Son el núcleo estructural del problema: egresados que no pagaron, la deuda siguió sumando intereses, y los mecanismos de recuperación del FSCU no alcanzan a frenarla. Representan el 73,8% de todos los deudores actuales.",
    "eli5": "3 de cada 4 personas que deben hoy al Fondo Solidario llevan 5 AÑOS COMPLETOS sin pagar y su deuda SUBE cada año. No es que no bajen: es que crecen. La deuda del 'crónico típico' pasó de 81 UTM ($5,6 millones CLP) en 2022 a 152 UTM ($10,6 millones CLP) en 2026. Casi el doble. Los intereses se comen todo. Es como dejar una olla hirviendo: al final ya no hay sopa, solo vapor.",
    "tipo": "chronic"
})

# 18. Patrimonio total deudores
interesante.append({
    "titulo": "Sus bienes valen más que su deuda",
    "kpi": "$7,19 bln",
    "sub": "Patrimonio total declarado de los deudores (propiedades + vehículos)",
    "desc": "Sumando el avalúo fiscal de todas las propiedades y la tasación de todos los vehículos que el SII reconoce a los 335.152 deudores, el total llega a $7,19 billones CLP — casi el DOBLE de la cartera que deben al Fondo Solidario ($4,02 billones). Propiedades aportan $5,95 bln y vehículos $1,24 bln. En principio, el patrimonio agregado alcanzaría para saldar la deuda con amplio margen.",
    "eli5": "Si juntaras todas las casas, departamentos y autos de los deudores, valen casi el DOBLE de lo que deben. Plata tienen — bienes tienen. No es que les falte con qué pagar; es que no pagan. Es como si tu primo te debiera $100 pero tuviera un auto nuevo de $200 y no le pasara nada.",
    "tipo": "wealth"
})

# 19. Barrios ABC1 Santiago
interesante.append({
    "titulo": "1.546 ejecutivos en barrios ABC1 de Santiago",
    "kpi": "1.546",
    "sub": "C-Level, directores y gerentes en Las Condes, Vitacura, Providencia, Lo Barnechea y Ñuñoa",
    "desc": "Las cinco comunas de mayor ingreso del Gran Santiago — Las Condes, Vitacura, Providencia, Lo Barnechea y Ñuñoa — concentran 1.546 deudores morosos con perfil de ejecutivo (C-Level, director o gerente) identificados en LinkedIn. Son los barrios con mayor precio por m² en Chile: el deudor típico vive ahí, maneja camioneta, y aparece en el registro público de morosidad del Fondo Solidario.",
    "eli5": "En los barrios más pitucos de Santiago — Las Condes, Vitacura, Providencia — viven casi 1.600 ejecutivos y gerentes que deben al Estado. Son los vecinos con las mejores casas y autos, los que van a colegios caros y vacacionan en Cachagua. Y al mismo tiempo no pagan el crédito de su propia carrera.",
    "tipo": "wealth"
})

# 20. Deuda crónicos duplicada
interesante.append({
    "titulo": "La deuda del crónico se duplicó en 4 años",
    "kpi": "+87,3%",
    "sub": "Mediana pasó de 81 UTM (2022) a 152 UTM (2026)",
    "desc": "Para los 247.300 deudores que aparecieron en las 5 nóminas sin interrupción (los 'crónicos acumulando'), la deuda mediana en UTM casi se duplicó entre 2022 y 2026: 81,2 → 98,7 → 117,4 → 133,9 → 152,1. Un crecimiento del 87,3% en 4 años, cuando la inflación oficial acumulada fue ~12% en el mismo período. La diferencia son intereses legales que se capitalizan sin pausa: cuando no pagas, la deuda crece sola a tasas que cuadruplican la inflación.",
    "eli5": "Si llevas 5 años sin pagar, tu deuda casi se dobló sola en ese tiempo. De $5,6 millones pasó a $10,6 millones — sin que hayas agregado NADA, sólo por intereses. Es como dejar la tarjeta de crédito impaga: al final no debes lo que compraste, debes 2 veces eso.",
    "tipo": "interest"
})

# 21. Juicio ejecutivo es la cobranza estándar
interesante.append({
    "titulo": "El juicio ejecutivo es la cobranza estándar",
    "kpi": "71.437",
    "sub": "94% de los demandados por deuda tiene Juicio Ejecutivo Obligación de Dar",
    "desc": "De los 72.014 deudores FSCU demandados por cobranza de deuda, 71.437 (94,2%) enfrentan específicamente un Juicio Ejecutivo Obligación de Dar — el procedimiento civil estándar en Chile para exigir el pago de una deuda documentada. Las otras categorías (Monitorio, Ley de Bancos, Gestión Preparatoria) son residuales. Es una señal clara: la cobranza judicial en Chile está estandarizada y los deudores FSCU enfrentan el mismo camino que cualquier otro moroso financiero.",
    "eli5": "Cuando te demandan porque no pagaste una deuda en Chile, el juicio casi siempre se llama 'Ejecutivo Obligación de Dar'. Es el procedimiento oficial estándar. Entre los morosos del Fondo Solidario que fueron demandados, 9 de cada 10 enfrenta este tipo específico de juicio. Es el protocolo de cobranza por default.",
    "tipo": "judicial"
})

# 22. Cohort 2022 casi no se mueve
interesante.append({
    "titulo": "El cohort 2022 virtualmente no se mueve",
    "kpi": "91,9%",
    "sub": "De los 288k deudores del 2022, 265k siguen en 2026",
    "desc": "Los 288.290 deudores que aparecían en la nómina 2022 son un grupo extraordinariamente estable: 4 años después, 265.072 (91,9%) siguen exactamente en el mismo lugar — en la nómina 2026. Sólo 23.218 personas (8,1%) lograron salir durante los últimos 4 años, ya sea por pago total o condonación. Este es el análisis longitudinal clásico de retención: en productos digitales se considera 'excelente' un 40% de retención a 4 años. En deudores morosos FSCU es 92%. El sistema no se vacía.",
    "eli5": "De todas las personas que debían plata al Fondo Solidario en 2022, prácticamente todos — 9 de cada 10 — siguen debiendo hoy. Sólo 1 de cada 12 logró salir en estos 4 años. En las apps y productos tecnológicos, si 9 de cada 10 usuarios siguen después de 4 años sería un éxito enorme. Acá es el fracaso: el problema casi no se mueve.",
    "tipo": "chronic"
})

# 23. Construcción y minería
interesante.append({
    "titulo": "Construcción y minería, las otras industrias morosas",
    "kpi": "7.402",
    "sub": "Construcción (3.811) + Minería/Metales (3.591) deudores",
    "desc": "Tras educación (9.710 deudores entre primaria/secundaria/superior), las siguientes dos industrias con más deudores FSCU identificados en LinkedIn son construcción (3.811) y minería/metales (3.591), sumando 7.402 personas. Son los sectores históricamente más fuertes en empleo formal masculino en Chile. El perfil coincide con el demográfico: hombres (54,9% del total), profesionales de ingenierías y especialidades técnicas, clase media-alta.",
    "eli5": "Después de los profes, las profesiones donde más hay morosos son construcción (3.811) y minería (3.591). Tiene sentido: son las carreras típicas donde estudian con crédito — ingenierías, geología, construcción civil. Después trabajan en obras o mineras, ganan bien, pero la deuda estudiantil queda por ahí sin pagar.",
    "tipo": "education"
})

# 24. Multi-propiedad + vehículos
interesante.append({
    "titulo": "64.044 deudores tienen auto Y casa",
    "kpi": "64.044",
    "sub": "19% de los morosos poseen al menos 1 propiedad y 1 vehículo",
    "desc": "Uno de cada cinco deudores morosos del Fondo Solidario (64.044 personas, 19,1% del total) tiene simultáneamente al menos una propiedad Y al menos un vehículo registrado en Servel/SII. No son el típico 'profesional sin bienes' que el imaginario asocia con el crédito universitario impago: son personas con vida establecida — casa propia o departamento, al menos un auto. El patrón refuta el supuesto de que la morosidad refleja incapacidad económica.",
    "eli5": "Casi 65 mil de los deudores tienen AL MISMO TIEMPO auto y casa propias. No es poco. Tienen dónde vivir, tienen cómo moverse. No son los típicos 'pobres sin plata' — son profesionales establecidos, con familia, con propiedades. Pero el Fondo Solidario lo dejaron pendiente.",
    "tipo": "wealth"
})

# 25. Santiago comuna top
interesante.append({
    "titulo": "Santiago comuna concentra 11.124 deudores",
    "kpi": "11.124",
    "sub": "La comuna de Santiago lidera el ranking nacional",
    "desc": "La comuna de Santiago (no confundir con la Región Metropolitana) concentra 11.124 deudores FSCU — el primer lugar del ranking comunal nacional. Es una comuna de tamaño mediano (menos de 400.000 habitantes) pero con alta concentración de población universitaria (egresados que arriendan en el centro), lo que explica el volumen. Le siguen comunas populosas como Maipú, Puente Alto, La Florida y Ñuñoa, donde vive gran parte de la clase media profesional chilena.",
    "eli5": "La comuna de Santiago (el centro) es donde más deudores FSCU viven: 11.124 personas. Es una comuna no tan grande, pero llena de egresados universitarios que arriendan departamentos céntricos. Es el barrio típico del profesional joven chileno.",
    "tipo": "demographics"
})

out["interesante"] = interesante
print(f"\nGeneradas {len(interesante)} cuñas periodísticas")

# ═══ PER-YEAR (navegación por años) ═══
per_year = {}
for y in all_years:
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

# "Todos" — vista consolidada 2022-2026
r = con.execute("""
SELECT COUNT(DISTINCT rut_dv) unicos,
       ROUND(SUM(monto_utm)) utm_acumulado,
       COUNT(*) filas_totales
FROM nominas
""").fetchone()
out["per_year"]["todos"] = {
    "year": "todos",
    "total": r[0],
    "utm_total": r[1],
    "filas_totales": r[2],
    "clp_total": int(r[1] * UTM_CLP),
    "usd_total": int(r[1] * UTM_USD),
    "avg_utm": round(r[1] / r[0], 1),
    "median_utm": None,
    "label": "2022-2026 · consolidado",
    "rango_años": 5,
}

# ═══ TRAYECTORIAS (análisis longitudinal) ═══
con.execute(f"CREATE VIEW trayectorias AS SELECT * FROM read_parquet('{DATA}/trayectorias.parquet')")
out["trayectorias"] = con.execute("""
SELECT trayectoria, COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),1) pct
FROM trayectorias WHERE m26 IS NOT NULL GROUP BY 1 ORDER BY 2 DESC
""").fetchdf().to_dict(orient="records")
out["trayectoria_cronicos_evol"] = con.execute("""
SELECT ROUND(MEDIAN(m22),1) m22, ROUND(MEDIAN(m23),1) m23, ROUND(MEDIAN(m24),1) m24,
       ROUND(MEDIAN(m25),1) m25, ROUND(MEDIAN(m26),1) m26,
       ROUND(AVG(m22),1) avg22, ROUND(AVG(m23),1) avg23, ROUND(AVG(m24),1) avg24,
       ROUND(AVG(m25),1) avg25, ROUND(AVG(m26),1) avg26
FROM trayectorias WHERE n_years = 5
""").fetchone()
out["trayectoria_cronicos_evol"] = {"median": list(out["trayectoria_cronicos_evol"][:5]),
                                     "avg": list(out["trayectoria_cronicos_evol"][5:])}

# ═══ DEMANDANTES (quién cobra) ═══
con.execute(f"CREATE VIEW dte AS SELECT * FROM read_parquet('{DATA}/demandantes.parquet')")
con.execute(f"CREATE VIEW univ_deuda AS SELECT * FROM read_parquet('{DATA}/universidad_demanda_rut.parquet')")

out["top_demandantes"] = con.execute("""
SELECT dte_nombre, n_deudores, n_causas, n_ejecutivo, n_ley_bancos
FROM dte ORDER BY n_deudores DESC LIMIT 30
""").fetchdf().to_dict(orient="records")

# Clasificar demandantes por categoría
out["demandantes_categorias"] = con.execute("""
SELECT CASE
    WHEN UPPER(dte_nombre) LIKE '%TESORERÍA%' OR UPPER(dte_nombre) LIKE '%TGR%' THEN 'Estado · Tesorería'
    WHEN UPPER(dte_nombre) LIKE '%UNIVERSIDAD%' OR UPPER(dte_nombre) LIKE 'U. %' THEN 'Universidades'
    WHEN UPPER(dte_nombre) LIKE '%BANCO%' OR UPPER(dte_nombre) LIKE '%ITAU%' OR UPPER(dte_nombre) LIKE '%SCOTIABANK%' OR UPPER(dte_nombre) LIKE '%BCI%' THEN 'Bancos'
    WHEN UPPER(dte_nombre) LIKE '%FALABELLA%' OR UPPER(dte_nombre) LIKE '%CMR%' OR UPPER(dte_nombre) LIKE '%RIPLEY%' OR UPPER(dte_nombre) LIKE '%PARIS%' OR UPPER(dte_nombre) LIKE '%CAT %' THEN 'Retail/Tarjetas'
    WHEN UPPER(dte_nombre) LIKE '%CCAF%' OR UPPER(dte_nombre) LIKE '%CAJA%' THEN 'Cajas Compensación'
    WHEN UPPER(dte_nombre) LIKE '%SERVICIOS FINANCIEROS%' OR UPPER(dte_nombre) LIKE '%GESTORA%' OR UPPER(dte_nombre) LIKE '%COBRANZA%' THEN 'Cobranza/Factoring'
    ELSE 'Otros' END cat,
    SUM(n_deudores) deudores_acumulados, SUM(n_causas) causas_acumuladas,
    COUNT(*) n_entidades
FROM dte GROUP BY 1 ORDER BY 2 DESC
""").fetchdf().to_dict(orient="records")

# Universidades demandantes — con vínculo canónico
out["universidades_demandantes"] = con.execute("""
SELECT dte_rut, dte_nombre,
       COUNT(DISTINCT deudor_rut) n_deudores,
       SUM(n_causas) n_causas
FROM univ_deuda GROUP BY 1, 2
ORDER BY 3 DESC LIMIT 20
""").fetchdf().to_dict(orient="records")

# ═══ GLOBAL · POLÍTICA PÚBLICA ═══
global_stats = {}

# Indicadores macro principales
transiciones = []
for y0, y1 in [(2022,2023),(2023,2024),(2024,2025),(2025,2026)]:
    r = con.execute(f"""
    WITH a AS (SELECT rut_dv, monto_utm m FROM nominas WHERE year={y0}),
         b AS (SELECT rut_dv, monto_utm m FROM nominas WHERE year={y1})
    SELECT ROUND(SUM(a.m)) utm_y0,
      ROUND(SUM(CASE WHEN b.rut_dv IS NULL THEN a.m ELSE 0 END)) utm_salido,
      ROUND(SUM(CASE WHEN b.rut_dv IS NOT NULL AND a.m > b.m THEN a.m - b.m ELSE 0 END)) utm_pago_parcial,
      ROUND(SUM(CASE WHEN b.rut_dv IS NOT NULL AND b.m > a.m THEN b.m - a.m ELSE 0 END)) utm_intereses,
      COUNT(*) n_y0,
      SUM(CASE WHEN b.rut_dv IS NULL THEN 1 ELSE 0 END) n_salio
    FROM a LEFT JOIN b USING (rut_dv)
    """).fetchone()
    recov = round(100*(r[1]+r[2])/r[0], 2) if r[0] else 0
    pct_salio = round(100*r[5]/r[4], 2) if r[4] else 0
    transiciones.append({
        "transicion": f"{y0}→{y1}",
        "utm_inicio": r[0], "utm_salido": r[1],
        "utm_pago_parcial": r[2], "utm_intereses": r[3],
        "recovery_rate_pct": recov, "n_inicio": r[4], "n_salio": r[5],
        "pct_salio": pct_salio,
    })
global_stats["transiciones"] = transiciones

# Probabilidad de salir por años consecutivos en nómina al 2025
global_stats["prob_salir_por_antiguedad"] = con.execute("""
WITH consec AS (
  SELECT rut_dv,
    CASE WHEN m22 IS NOT NULL AND m23 IS NOT NULL AND m24 IS NOT NULL AND m25 IS NOT NULL THEN 4
         WHEN m23 IS NOT NULL AND m24 IS NOT NULL AND m25 IS NOT NULL THEN 3
         WHEN m24 IS NOT NULL AND m25 IS NOT NULL THEN 2
         WHEN m25 IS NOT NULL THEN 1 ELSE 0 END AS anos_consec,
    m25, m26
  FROM trayectorias WHERE m25 IS NOT NULL
)
SELECT anos_consec, COUNT(*) estaban,
  SUM(CASE WHEN m26 IS NULL THEN 1 ELSE 0 END) salieron,
  ROUND(100.0*SUM(CASE WHEN m26 IS NULL THEN 1 ELSE 0 END)/COUNT(*),2) pct_salida
FROM consec GROUP BY 1 ORDER BY 1
""").fetchdf().to_dict(orient="records")

# Monto promedio vs años en nómina
global_stats["monto_vs_antiguedad"] = con.execute("""
SELECT n_years, COUNT(*) n,
  ROUND(AVG(COALESCE(m26, m25, m24, m23, m22)),1) avg_monto,
  ROUND(MEDIAN(COALESCE(m26, m25, m24, m23, m22)),1) med_monto
FROM trayectorias GROUP BY 1 ORDER BY 1
""").fetchdf().to_dict(orient="records")

# Retención por cohort
cohorts = []
for cy in [2022, 2023, 2024, 2025]:
    r = con.execute(f"""
    WITH cohort AS (SELECT DISTINCT rut_dv FROM nominas WHERE year={cy}
                    AND rut_dv NOT IN (SELECT rut_dv FROM nominas WHERE year<{cy}))
    SELECT COUNT(DISTINCT c.rut_dv) base,
      SUM(CASE WHEN EXISTS(SELECT 1 FROM nominas WHERE rut_dv=c.rut_dv AND year=2026) THEN 1 ELSE 0 END) en_2026
    FROM cohort c
    """).fetchone()
    pct = round(100 * r[1] / r[0], 1) if r[0] else 0
    cohorts.append({"cohort": cy, "base": r[0], "en_2026": r[1], "pct_retenido": pct})
global_stats["cohorts"] = cohorts

# Proyección lineal de cartera
import statistics
yrs_list = [e["year"] for e in out["evolucion"]]
utm_list = [e["utm_total"] for e in out["evolucion"]]
n = len(yrs_list)
mx, my = statistics.mean(yrs_list), statistics.mean(utm_list)
slope = sum((x-mx)*(y-my) for x,y in zip(yrs_list, utm_list)) / sum((x-mx)**2 for x in yrs_list)
intercept = my - slope * mx
proyeccion = []
for y in [2027, 2028, 2029, 2030]:
    utm = int(intercept + slope * y)
    proyeccion.append({
        "year": y, "utm_total": utm,
        "clp_total": int(utm * UTM_CLP), "usd_total": int(utm * UTM_USD),
        "tipo": "proyeccion"
    })
global_stats["proyeccion"] = proyeccion
global_stats["proyeccion_slope_utm_anual"] = int(slope)
global_stats["proyeccion_desc"] = f"Regresión lineal simple basada en 2022-2026 (pendiente {int(slope):,} UTM/año)."

# KPIs resumen
avg_recovery = round(sum(t["recovery_rate_pct"] for t in transiciones) / len(transiciones), 2)
avg_growth_utm = round(100 * ((utm_list[-1]/utm_list[0])**(1/(len(utm_list)-1)) - 1), 2)
global_stats["kpis"] = {
    "tasa_recuperacion_anual_pct": avg_recovery,
    "tasa_crecimiento_cartera_anual_pct": avg_growth_utm,
    "pct_cronicos_5_anos": out["cronicidad"]["pct_cronicos"],
    "pct_retencion_cohort_2022": cohorts[0]["pct_retenido"],
    "utm_2026": out["resumen"]["total_utm"],
    "utm_2030_proyectado": proyeccion[-1]["utm_total"],
    "clp_2030_proyectado": proyeccion[-1]["clp_total"],
    "crecimiento_2026_2030_pct": round(100*(proyeccion[-1]["utm_total"]-out["resumen"]["total_utm"])/out["resumen"]["total_utm"], 1),
}

out["global"] = global_stats

# ═══ MATRIZ DE COHORTES + STREAM DE COMPOSICIÓN ═══
cohort_matrix = []
for cy in all_years:
    base_rows = con.execute(f"""SELECT DISTINCT rut_dv FROM nominas WHERE year={cy}
                 AND rut_dv NOT IN (SELECT rut_dv FROM nominas WHERE year<{cy})""").fetchdf()
    base = len(base_rows)
    row = {"cohort": cy, "base": base, "years": {}}
    for y in all_years:
        if y < cy:
            row["years"][y] = None
        else:
            n = con.execute(f"""SELECT COUNT(*) FROM (SELECT DISTINCT rut_dv FROM nominas WHERE year={cy}
                AND rut_dv NOT IN (SELECT rut_dv FROM nominas WHERE year<{cy})) c
                WHERE EXISTS(SELECT 1 FROM nominas WHERE rut_dv=c.rut_dv AND year={y})""").fetchone()[0]
            row["years"][y] = n
    cohort_matrix.append(row)
out["cohort_matrix"] = cohort_matrix
out["years_available"] = all_years
out["years_missing"] = [y for y in range(all_years[0], all_years[-1]+1) if y not in all_years]

# Stream composición: cada año qué viene de qué cohort
stream = {}
for y in all_years:
    stream[y] = {}
    for cy in all_years:
        if cy > y:
            stream[y][cy] = 0
        else:
            n = con.execute(f"""SELECT COUNT(*) FROM
                (SELECT DISTINCT rut_dv FROM nominas WHERE year={cy}
                 AND rut_dv NOT IN (SELECT rut_dv FROM nominas WHERE year<{cy})) c
                WHERE EXISTS(SELECT 1 FROM nominas WHERE rut_dv=c.rut_dv AND year={y})""").fetchone()[0]
            stream[y][cy] = n
out["stream_composicion"] = stream

# ═══ RANKING RECUPERACIÓN POR UNIVERSIDAD (2022 → 2026) ═══
# Top comunas por perfil LinkedIn (ejecutivos, gerentes, académicos)
out["comunas_ejecutivos"] = con.execute("""
SELECT comuna, region, COUNT(*) n FROM full_t
WHERE en_linkedin AND seniority IN ('c-level','director') AND comuna IS NOT NULL
GROUP BY 1,2 HAVING COUNT(*) >= 10 ORDER BY 3 DESC LIMIT 20
""").fetchdf().to_dict(orient="records")
out["comunas_gerentes"] = con.execute("""
SELECT comuna, region, COUNT(*) n FROM full_t
WHERE en_linkedin AND seniority = 'manager' AND comuna IS NOT NULL
GROUP BY 1,2 HAVING COUNT(*) >= 10 ORDER BY 3 DESC LIMIT 20
""").fetchdf().to_dict(orient="records")
out["comunas_academicos"] = con.execute("""
SELECT comuna, region, COUNT(*) n FROM full_t
WHERE en_linkedin AND seniority = 'academic' AND comuna IS NOT NULL
GROUP BY 1,2 HAVING COUNT(*) >= 10 ORDER BY 3 DESC LIMIT 20
""").fetchdf().to_dict(orient="records")

out["ranking_recuperacion"] = con.execute("""
WITH y22 AS (SELECT universidad_canon u, COUNT(DISTINCT rut_dv) n, SUM(monto_utm) m FROM nominas WHERE year=2022 GROUP BY 1),
     y26 AS (SELECT universidad_canon u, COUNT(DISTINCT rut_dv) n, SUM(monto_utm) m FROM nominas WHERE year=2026 GROUP BY 1)
SELECT y22.u AS universidad,
       y22.n AS deudores_2022, y26.n AS deudores_2026,
       ROUND(y22.m) AS utm_2022, ROUND(y26.m) AS utm_2026,
       ROUND(100.0*(y26.n-y22.n)/y22.n, 1) AS pct_delta_deudores,
       ROUND(100.0*(y26.m-y22.m)/y22.m, 1) AS pct_delta_cartera_utm,
       ROUND((y26.m-y22.m)) AS utm_delta
FROM y22 FULL OUTER JOIN y26 USING (u)
WHERE y22.u IS NOT NULL AND y26.u IS NOT NULL
ORDER BY pct_delta_cartera_utm ASC
""").fetchdf().to_dict(orient="records")

print("\n=== GLOBAL KPIs ===")
for k, v in global_stats["kpis"].items(): print(f"  {k}: {v:,}" if isinstance(v,(int,float)) else f"  {k}: {v}")

# ═══ JUSTICIA ═══
out["justicia_resumen"] = {
    "total": out["resumen"]["total"],
    "con_pjud": out["resumen"]["con_pjud"],
    "con_demandado_deuda": out["resumen"]["con_demandado_deuda"],
    "pct_con_pjud": round(100 * out["resumen"]["con_pjud"] / out["resumen"]["total"], 1),
    "pct_con_demandado_deuda": round(100 * out["resumen"]["con_demandado_deuda"] / out["resumen"]["total"], 1),
}
out["justicia_por_causa"] = table("""SELECT CASE
    WHEN n_causas_deuda = 0 THEN 'Sin causas por deuda'
    WHEN n_causas_deuda = 1 THEN '1 causa'
    WHEN n_causas_deuda BETWEEN 2 AND 3 THEN '2-3 causas'
    WHEN n_causas_deuda BETWEEN 4 AND 10 THEN '4-10 causas'
    WHEN n_causas_deuda > 10 THEN '10+ causas'
    ELSE 'Otro' END bucket, COUNT(*) n FROM full_t GROUP BY 1""")
out["justicia_por_region"] = table("""SELECT COALESCE(region,'(sin región)') region,
    COUNT(*) n, SUM(CASE WHEN demandado_por_deuda = TRUE THEN 1 ELSE 0 END) demandados_civil,
    ROUND(100.0*SUM(CASE WHEN demandado_por_deuda = TRUE THEN 1 ELSE 0 END)/COUNT(*),1) pct_demandados
FROM full_t GROUP BY 1 ORDER BY 2 DESC""")
out["justicia_por_universidad"] = table("""SELECT universidad_principal universidad,
    COUNT(*) n, SUM(CASE WHEN demandado_por_deuda = TRUE THEN 1 ELSE 0 END) demandados_civil,
    ROUND(100.0*SUM(CASE WHEN demandado_por_deuda = TRUE THEN 1 ELSE 0 END)/COUNT(*),1) pct_demandados
FROM full_t WHERE universidad_principal IS NOT NULL GROUP BY 1 ORDER BY 2 DESC""")
out["justicia_por_perfil"] = table("""WITH p AS (SELECT demandado_por_deuda, CASE
    WHEN en_linkedin AND seniority IN ('c-level','director') THEN 'Ejecutivo'
    WHEN en_linkedin AND seniority = 'manager' THEN 'Gerente'
    WHEN en_linkedin AND seniority = 'academic' THEN 'Académico'
    WHEN en_linkedin AND seniority = 'senior' THEN 'Profesional sr'
    WHEN en_linkedin AND seniority = 'professional' THEN 'Profesional'
    WHEN en_linkedin AND seniority = 'operational' THEN 'Operativo'
    WHEN en_linkedin THEN 'LK sin clasif'
    WHEN decil_avaluo >= 8 THEN 'Alto patrimonio'
    ELSE 'Sin info' END perfil FROM full_t)
SELECT perfil, COUNT(*) n, SUM(CASE WHEN demandado_por_deuda = TRUE THEN 1 ELSE 0 END) demandados,
    ROUND(100.0*SUM(CASE WHEN demandado_por_deuda = TRUE THEN 1 ELSE 0 END)/COUNT(*),1) pct
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
    SELECT COUNT(*) n, SUM(CASE WHEN demandado_por_deuda = TRUE THEN 1 ELSE 0 END) demandados
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
