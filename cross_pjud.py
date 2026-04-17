"""Cruza los 370k RUTs FSCU contra PJUD_Unholster (Grupo Defensa).

Output: data/deudores_pjud.parquet
    rut_dv,
    n_civil,            # causas civiles totales
    n_civil_ddo,        # como demandado
    n_civil_dte,        # como demandante
    n_laboral,          # causas laborales totales
    n_laboral_ddo,      # como demandado
    n_laboral_dte,      # como demandante
    ultimo_year,        # año más reciente de causa (civil)

La conexión se mantiene abierta en una sola sesión (#temp table).
"""
import pymssql
import pandas as pd
import duckdb
import time
from pathlib import Path

DATA = Path("/Users/antonio/deudores-fscu/data")
OUT = DATA / "deudores_pjud.parquet"

# Load RUTs desde parquet
con_local = duckdb.connect(":memory:")
df_ruts = con_local.execute(f"""
    SELECT DISTINCT rut_dv FROM read_parquet('{DATA}/deudores_enriched.parquet')
    WHERE rut_dv IS NOT NULL
""").df()
print(f"RUTs a cruzar: {len(df_ruts):,}")

t0 = time.time()
con = pymssql.connect(
    server='ec2-3-135-172-36.us-east-2.compute.amazonaws.com',
    user='User_Unholster', password='*cdwq%=qYA?i#f3', database='PJUD_Unholster',
    timeout=600, login_timeout=30,
)
cur = con.cursor()

# Crear temp table
print("Creando #deudores...")
cur.execute("""
CREATE TABLE #deudores (
    rut_dv VARCHAR(15) COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY
)
""")

# Bulk insert en batches
print("Insertando RUTs...")
BATCH = 1000
ruts = df_ruts['rut_dv'].tolist()
for i in range(0, len(ruts), BATCH):
    chunk = ruts[i:i+BATCH]
    values = ",".join("(%s)" for _ in chunk)
    cur.executemany("INSERT INTO #deudores (rut_dv) VALUES (%s)", [(r,) for r in chunk])
    if (i // BATCH) % 20 == 0:
        print(f"  {i+len(chunk):,} / {len(ruts):,} ({(time.time()-t0):.0f}s)")
con.commit()
cur.execute("SELECT COUNT(*) FROM #deudores")
print(f"  en staging: {cur.fetchone()[0]:,} ({(time.time()-t0):.0f}s)")

# Query civil
print("\n[1/2] Joining con litigantes_civil (22M rows)...")
cur.execute("""
SELECT
    d.rut_dv AS rut_dv,
    COUNT(DISTINCT l.CRR_IdCausa) AS n_civil,
    SUM(CASE WHEN l.participante IN ('DDO.','AB.DDO','AP.DDO') THEN 1 ELSE 0 END) AS n_civil_ddo,
    SUM(CASE WHEN l.participante IN ('DTE.','AB.DTE','AP.DTE') THEN 1 ELSE 0 END) AS n_civil_dte,
    SUM(CASE WHEN l.participante IN ('ACRDOR','AB.ACR') THEN 1 ELSE 0 END) AS n_civil_acreedor,
    SUM(CASE WHEN l.participante = 'DDO.' THEN 1 ELSE 0 END) AS n_civil_ddo_directo,
    MAX(TRY_CAST(RIGHT(l.rol, 4) AS INT)) AS ultimo_year_civil
FROM #deudores d
INNER JOIN dbo.litigantes_civil l ON l.rut = d.rut_dv
GROUP BY d.rut_dv
""")
rows_civil = cur.fetchall()
cols_civil = [c[0] for c in cur.description]
df_civil = pd.DataFrame(rows_civil, columns=cols_civil)
print(f"  deudores con match civil: {len(df_civil):,} ({(time.time()-t0):.0f}s)")

# Query laboral
print("\n[2/2] Joining con litigantes_laboral (1.2M rows)...")
cur.execute("""
SELECT
    d.rut_dv AS rut_dv,
    COUNT(*) AS n_laboral,
    SUM(CASE WHEN l.sujeto LIKE 'DDO%' THEN 1 ELSE 0 END) AS n_laboral_ddo,
    SUM(CASE WHEN l.sujeto LIKE 'DTE%' THEN 1 ELSE 0 END) AS n_laboral_dte
FROM #deudores d
INNER JOIN dbo.litigantes_laboral l ON l.rut = d.rut_dv
GROUP BY d.rut_dv
""")
rows_lab = cur.fetchall()
cols_lab = [c[0] for c in cur.description]
df_lab = pd.DataFrame(rows_lab, columns=cols_lab)
print(f"  deudores con match laboral: {len(df_lab):,} ({(time.time()-t0):.0f}s)")

con.close()

# Merge
df = df_ruts.merge(df_civil, on='rut_dv', how='left').merge(df_lab, on='rut_dv', how='left')
num_cols = [c for c in df.columns if c != 'rut_dv']
for c in num_cols: df[c] = df[c].fillna(0).astype(int)
df['en_pjud'] = (df['n_civil'] > 0) | (df['n_laboral'] > 0)

df.to_parquet(OUT, index=False)
print(f"\nEscrito {OUT}")
print(f"Total deudores: {len(df):,}")
print(f"Con alguna causa (civil o laboral): {df['en_pjud'].sum():,} ({100*df['en_pjud'].mean():.1f}%)")
print(f"Con causa civil: {(df['n_civil']>0).sum():,}")
print(f"Como demandado civil: {(df['n_civil_ddo_directo']>0).sum():,}")
print(f"Con causa laboral: {(df['n_laboral']>0).sum():,}")
print(f"Causas civiles promedio (entre los con match): {df[df['n_civil']>0]['n_civil'].mean():.1f}")
