"""Valida los RUTs y nombres de nuestro consolidado contra Audiencias (OpenSearch).

Para cada RUT único del consolidado 2015-2026:
- Query a persona_natural_v2 por RUT
- Compara apellido paterno + materno + primer nombre con lo que tenemos
- Categoriza: exact_match / partial / wrong_person / not_found

Output: data/validation_ruts.parquet y resumen en stdout.
"""
import requests, urllib3, json, time
import pandas as pd
import duckdb
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
urllib3.disable_warnings()

DATA = Path("/Users/antonio/deudores-fscu/data")
OUT = DATA / "validation_ruts.parquet"
BATCH = 500
PORTS = list(range(9201, 9210))  # 9 tuneles
INDEX = "persona_natural_v2"

def normalize(s):
    return str(s or '').upper().strip()

def load_ruts():
    con = duckdb.connect(":memory:")
    con.execute(f"""CREATE VIEW n AS SELECT * FROM read_parquet('{DATA}/nominas_consolidado.parquet')""")
    # Un registro por RUT, usa el más reciente (year más alto)
    r = con.execute("""
    SELECT rut_dv, rut, apellido_paterno, apellido_materno, nombres, year
    FROM (
      SELECT rut_dv, rut, apellido_paterno, apellido_materno, nombres, year,
             ROW_NUMBER() OVER (PARTITION BY rut_dv ORDER BY year DESC) rn
      FROM n
    ) WHERE rn = 1
    """).fetchdf()
    return r

def query_batch(ruts, port):
    url = f"https://localhost:{port}/{INDEX}/_search"
    body = {
        "query": {"terms": {"rut": ruts}},
        "size": len(ruts),
        "_source": ["rut", "nombre_completo", "apellidos", "nombres"],
    }
    r = requests.post(url, json=body, verify=False, timeout=60)
    r.raise_for_status()
    hits = r.json().get("hits", {}).get("hits", [])
    # build dict rut -> first record
    d = {}
    for h in hits:
        src = h["_source"]
        rut = src.get("rut")
        if not rut or rut in d: continue
        # Take first namespace value
        nc = src.get("nombre_completo", [])
        nombre_full = nc[0]["value"] if nc else ''
        aps = src.get("apellidos", [])
        ap_full = aps[0]["value"] if aps else ''
        noms = src.get("nombres", [])
        nom_full = noms[0]["value"] if noms else ''
        d[rut] = {"nombre_completo": nombre_full, "apellidos_full": ap_full, "nombres_full": nom_full}
    return d

def classify(row, record):
    """Compara lo que tenemos con lo que Audiencias dice."""
    if record is None:
        return "not_found"
    our_ap_p = normalize(row["apellido_paterno"])
    our_ap_m = normalize(row["apellido_materno"])
    our_nom = normalize(row["nombres"]).split()[0] if row["nombres"] else ''
    aud_apell = normalize(record.get("apellidos_full") or record.get("nombre_completo", ""))
    aud_nom_full = normalize(record.get("nombres_full") or '')
    aud_nombre_completo = normalize(record.get("nombre_completo") or '')
    # Exact match: apellido_p AND apellido_m aparecen en los apellidos de Audiencias
    if our_ap_p and our_ap_p in aud_apell and our_ap_m and our_ap_m in aud_apell:
        # Bonus: primer nombre también coincide
        if our_nom and (our_nom in aud_nom_full or our_nom in aud_nombre_completo):
            return "exact_match"
        return "partial_match"
    # Sólo ap_paterno coincide
    if our_ap_p and our_ap_p in aud_apell:
        return "partial_ap_paterno"
    # RUT existe pero nombre totalmente distinto
    return "wrong_person"

def main():
    df = load_ruts()
    print(f"Total RUTs únicos: {len(df):,}", flush=True)

    # Lista de RUTs
    ruts_all = df['rut'].astype(str).tolist()
    rut_to_row = df.set_index('rut').to_dict(orient='index')

    # Procesar en batches
    batches = [ruts_all[i:i+BATCH] for i in range(0, len(ruts_all), BATCH)]
    print(f"Batches: {len(batches)} de {BATCH} RUTs", flush=True)

    results = {}  # rut -> record or None
    t0 = time.time()
    def work(idx_batch):
        idx, batch = idx_batch
        port = PORTS[idx % len(PORTS)]
        try:
            d = query_batch(batch, port)
            return {r: d.get(r) for r in batch}
        except Exception as e:
            return {r: None for r in batch}

    done = 0
    with ThreadPoolExecutor(max_workers=len(PORTS)) as ex:
        futures = [ex.submit(work, (i, b)) for i, b in enumerate(batches)]
        for fut in as_completed(futures):
            batch_results = fut.result()
            results.update(batch_results)
            done += 1
            if done % 20 == 0 or done == len(batches):
                dt = time.time() - t0
                eta = (len(batches) - done) * dt / done
                print(f"  {done}/{len(batches)} batches · {dt:.0f}s · ETA {eta/60:.1f}min · hits so far: {sum(1 for v in results.values() if v):,}", flush=True)

    # Clasificar
    out_rows = []
    for rut, our_row in rut_to_row.items():
        rec = results.get(str(rut))
        category = classify(our_row, rec)
        out_rows.append({
            'rut': rut,
            'rut_dv': our_row['rut_dv'],
            'year_ultimo': our_row['year'],
            'nuestro_ap_p': our_row['apellido_paterno'],
            'nuestro_ap_m': our_row['apellido_materno'],
            'nuestros_nombres': our_row['nombres'],
            'audiencias_nombre_completo': rec.get('nombre_completo') if rec else None,
            'category': category,
        })
    df_out = pd.DataFrame(out_rows)
    df_out.to_parquet(OUT, index=False)

    # Resumen
    print(f"\n=== Clasificación ===")
    print(df_out['category'].value_counts().to_string())
    print(f"\nTotal validados: {len(df_out):,}")
    print(f"Escrito {OUT}")

if __name__ == '__main__':
    main()
