"""Cruza 370k RUTs FSCU contra PJUD_Unholster por chunks de 2000.

Estrategia: en vez de staging table + join sobre 22M rows, hacemos queries
paralelas con IN (rut, rut, ...) de 2000 RUTs cada una contra el índice natural
de litigantes_civil.rut. 185 queries × ~2s cada una.

Output: data/deudores_pjud.parquet
"""
import pymssql
import pandas as pd
import duckdb
import time
import sys
from pathlib import Path

DATA = Path("/Users/antonio/deudores-fscu/data")
OUT = DATA / "deudores_pjud.parquet"
CHUNK = 2000

def load_ruts():
    con = duckdb.connect(":memory:")
    df = con.execute(f"""
        SELECT DISTINCT rut_dv FROM read_parquet('{DATA}/deudores_enriched.parquet')
        WHERE rut_dv IS NOT NULL
    """).df()
    return df['rut_dv'].tolist()

def connect():
    return pymssql.connect(
        server='ec2-3-135-172-36.us-east-2.compute.amazonaws.com',
        user='User_Unholster', password='*cdwq%=qYA?i#f3', database='PJUD_Unholster',
        timeout=300, login_timeout=30,
    )

def query_chunk(cur, ruts, sql_template):
    ph = ','.join(['%s'] * len(ruts))
    sql = sql_template.format(ph)
    cur.execute(sql, ruts)
    return cur.fetchall()

def main():
    ruts = load_ruts()
    print(f"RUTs: {len(ruts):,}", flush=True)

    # SQL civil — agrega por rut
    SQL_CIVIL = """
    SELECT l.rut AS rut_dv,
        COUNT(DISTINCT l.CRR_IdCausa) AS n_civil,
        SUM(CASE WHEN l.participante IN ('DDO.','AB.DDO','AP.DDO') THEN 1 ELSE 0 END) AS n_civil_ddo,
        SUM(CASE WHEN l.participante IN ('DTE.','AB.DTE','AP.DTE') THEN 1 ELSE 0 END) AS n_civil_dte,
        SUM(CASE WHEN l.participante IN ('ACRDOR','AB.ACR') THEN 1 ELSE 0 END) AS n_civil_acreedor,
        MAX(TRY_CAST(RIGHT(l.rol,4) AS INT)) AS ultimo_year_civil
    FROM dbo.litigantes_civil l
    WHERE l.rut IN ({})
    GROUP BY l.rut
    """
    SQL_LAB = """
    SELECT l.rut AS rut_dv,
        COUNT(*) AS n_laboral,
        SUM(CASE WHEN l.sujeto LIKE 'DDO%' THEN 1 ELSE 0 END) AS n_laboral_ddo,
        SUM(CASE WHEN l.sujeto LIKE 'DTE%' THEN 1 ELSE 0 END) AS n_laboral_dte
    FROM dbo.litigantes_laboral l
    WHERE l.rut IN ({})
    GROUP BY l.rut
    """

    all_civil = {}
    all_lab = {}

    con = connect()
    cur = con.cursor()
    t0 = time.time()

    nchunks = (len(ruts) + CHUNK - 1) // CHUNK
    for i in range(0, len(ruts), CHUNK):
        batch = ruts[i:i+CHUNK]
        idx = i // CHUNK + 1
        # Civil
        rows = query_chunk(cur, batch, SQL_CIVIL)
        for r in rows: all_civil[r[0]] = r
        # Laboral
        rows = query_chunk(cur, batch, SQL_LAB)
        for r in rows: all_lab[r[0]] = r
        if idx % 5 == 0 or idx == nchunks:
            dt = time.time() - t0
            eta = dt * (nchunks - idx) / max(idx, 1)
            print(f"  chunk {idx}/{nchunks} · civil={len(all_civil):,} lab={len(all_lab):,} · {dt:.0f}s · ETA {eta/60:.1f}min", flush=True)
    con.close()

    # merge
    df_civil = pd.DataFrame(all_civil.values(), columns=['rut_dv','n_civil','n_civil_ddo','n_civil_dte','n_civil_acreedor','ultimo_year_civil'])
    df_lab = pd.DataFrame(all_lab.values(), columns=['rut_dv','n_laboral','n_laboral_ddo','n_laboral_dte'])

    df = pd.DataFrame({'rut_dv': ruts}).merge(df_civil, on='rut_dv', how='left').merge(df_lab, on='rut_dv', how='left')
    for c in df.columns:
        if c != 'rut_dv': df[c] = df[c].fillna(0).astype(int)
    df['en_pjud'] = (df['n_civil'] > 0) | (df['n_laboral'] > 0)

    df.to_parquet(OUT, index=False)
    print(f"\nEscrito {OUT}")
    print(f"Total: {len(df):,} · con causa: {df['en_pjud'].sum():,} ({100*df['en_pjud'].mean():.1f}%)")
    print(f"Con causa civil: {(df['n_civil']>0).sum():,}")
    print(f"Como demandado civil (DDO.): {(df['n_civil_ddo']>0).sum():,}")
    print(f"Con causa laboral: {(df['n_laboral']>0).sum():,}")

if __name__ == '__main__':
    main()
