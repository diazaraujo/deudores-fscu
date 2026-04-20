"""Parser para nóminas CRUCH históricas 2015-2017 (formato dual-column)
y 2021 XLSX. Los PDFs 2015-2017 tienen 2 entradas de deudor por línea.

Formato 2015: RUT DV AP_P AP_M NOMBRE(...) UNIV_CODE MONTO (duplicado por línea)
Formato 2016-2017: RUTDV(concatenado) AP_P AP_M NOMBRE(...) UNIV_CODE MONTO

Universidades (códigos cortos → canónicos):
"""
import sys, re
import pdfplumber, openpyxl
import pandas as pd
from pathlib import Path

DATA = Path("/Users/antonio/deudores-fscu/data")
RAW = DATA / "nominas_raw"
OUT = DATA / "nominas"

# Mapa de códigos cortos → nombres canónicos (mismos usados en normalize_nominas)
CODE_MAP = {
    'UCHILE': 'U. de Chile', 'UdeChile': 'U. de Chile',
    'PUC': 'PUC de Chile', 'UCDECHILE': 'PUC de Chile',
    'UCV': 'PUC de Valparaíso', 'UCVALPO': 'PUC de Valparaíso',
    'UDEC': 'U. de Concepción', 'UdeConcepcion': 'U. de Concepción',
    'USACH': 'U. de Santiago',
    'UTFSM': 'UTFSM',
    'UACH': 'U. Austral',
    'UCN': 'U. Católica del Norte',
    'UCSC': 'U. Católica Santísima Concepción',
    'UCT': 'U. Católica de Temuco',
    'UCM': 'U. Católica del Maule', 'UCMAULE': 'U. Católica del Maule',
    'UV': 'U. de Valparaíso', 'UVALPO': 'U. de Valparaíso',
    'UANTOF': 'U. de Antofagasta',
    'UDA': 'U. de Atacama', 'UATACAMA': 'U. de Atacama',
    'UFRO': 'U. de La Frontera',
    'USERENA': 'U. de La Serena',
    'UMAG': 'U. de Magallanes',
    'UPLA': 'U. de Playa Ancha',
    'UTALCA': 'U. de Talca',
    'UTA': 'U. de Tarapacá',
    'UBIOBIO': 'U. del Bío-Bío', 'UBB': 'U. del Bío-Bío',
    'UMCE': 'UMCE',
    'UTEM': 'UTEM',
    'UNAP': 'U. Arturo Prat',
    'ULAGOS': 'U. de Los Lagos',
    'UOH': "U. de O'Higgins",
}

def canonicalize(u):
    key = str(u).strip().upper()
    return CODE_MAP.get(key, str(u).strip())

MONTO_RE = re.compile(r'^\d+[.,]\d+$')
RUT_DV_CONCAT = re.compile(r'^(\d{6,9})([0-9Kk])$')  # 2016-2017: RUT+DV stuck
UNIV_CODES = set(CODE_MAP.keys())

def parse_line_dual(line, combined_rut=None):
    """Parsea una línea con 1-2 entradas de deudor.
    Detecta dinámicamente si RUT y DV vienen separados o concatenados."""
    tokens = line.split()
    results = []
    start = 0
    i = 0
    while i < len(tokens):
        if MONTO_RE.match(tokens[i]):
            monto_tok = tokens[i]
            chunk = tokens[start:i+1]
            if len(chunk) < 5:
                start = i + 1; i += 1
                continue
            try:
                monto_f = float(monto_tok.replace(',', '.'))
            except ValueError:
                start = i + 1; i += 1
                continue
            # Identificar univ code (penúltimo token antes de monto)
            univ = chunk[-2] if chunk[-2].upper() in UNIV_CODES else None
            if univ is None:
                for idx in range(len(chunk)-2, max(0, len(chunk)-6), -1):
                    tok = chunk[idx]
                    if tok.isalpha() and 2 <= len(tok) <= 10 and tok.isupper():
                        univ = tok; break
            if univ is None:
                start = i + 1; i += 1
                continue
            try:
                univ_idx = len(chunk) - 2  # penúltimo (antes del monto)
                while univ_idx > 2 and chunk[univ_idx].upper() != univ.upper():
                    univ_idx -= 1
            except ValueError:
                start = i + 1; i += 1
                continue

            # Detectar formato dinámicamente por chunk[1]
            dv_sep = len(chunk[1]) == 1 and (chunk[1].isdigit() or chunk[1].upper() == 'K')
            if dv_sep and chunk[0].isdigit() and len(chunk[0]) >= 6:
                # Formato separado: RUT DV AP_P AP_M NOMBRE...
                rut = chunk[0]
                dv = chunk[1].upper()
                ap_p = chunk[2] if len(chunk) > 2 else ''
                ap_m = chunk[3] if len(chunk) > 3 else ''
                nombres = ' '.join(chunk[4:univ_idx]) if univ_idx > 4 else ''
            else:
                # Formato combinado: RUTDV AP_P AP_M NOMBRE...
                m = RUT_DV_CONCAT.match(chunk[0])
                if not m:
                    start = i + 1; i += 1
                    continue
                rut, dv = m.group(1), m.group(2).upper()
                ap_p = chunk[1] if len(chunk) > 1 else ''
                ap_m = chunk[2] if len(chunk) > 2 else ''
                nombres = ' '.join(chunk[3:univ_idx]) if univ_idx > 3 else ''

            # Filtros de sanidad
            if not rut.isdigit() or len(rut) < 6:
                start = i + 1; i += 1
                continue
            if not (ap_p and ap_p[0].isalpha()):  # rechaza ap_p tipo "0"
                start = i + 1; i += 1
                continue

            results.append({
                'rut': rut, 'dv': dv,
                'apellido_paterno': ap_p, 'apellido_materno': ap_m,
                'nombres': nombres, 'monto_utm': monto_f, 'universidad': univ,
            })
            start = i + 1
        i += 1
    return results

def parse_pdf_historical(year, path, combined_rut=None):
    pdf = pdfplumber.open(path)
    n = len(pdf.pages)
    print(f'[{year}] PDF {n} páginas (detección dinámica RUT/DV)', flush=True)
    rows = []
    for i, page in enumerate(pdf.pages):
        t = page.extract_text() or ''
        for ln in t.splitlines():
            for r in parse_line_dual(ln):
                r['year'] = year
                rows.append(r)
        if (i+1) % 100 == 0 or i == n - 1:
            print(f'[{year}]  {i+1}/{n}  rows={len(rows):,}', flush=True)
    pdf.close()
    return rows

def parse_xlsx_2021(path):
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    out = []
    for r in rows[1:]:
        if not r[0] or not r[5]: continue
        rut = str(r[0]).strip().lstrip('0')
        dv = str(r[1]).strip().upper() if r[1] is not None else ''
        if not (rut.isdigit() and (dv.isdigit() or dv == 'K')): continue
        try: monto = float(r[5])
        except (ValueError, TypeError): continue
        out.append({
            'rut': rut, 'dv': dv,
            'apellido_paterno': str(r[2] or '').strip(),
            'apellido_materno': str(r[3] or '').strip(),
            'nombres': str(r[4] or '').strip(),
            'monto_utm': monto,
            'universidad': str(r[6] or '').strip(),
            'year': 2021,
        })
    return out

def save(rows, year):
    df = pd.DataFrame(rows)
    df['rut_dv'] = df['rut'] + '-' + df['dv']
    df['universidad_canon'] = df['universidad'].apply(canonicalize)
    out = OUT / f'{year}.parquet'
    df.to_parquet(out, index=False)
    print(f'[{year}] Escrito {out}')
    print(f'[{year}]  filas: {len(df):,}  RUTs únicos: {df["rut"].nunique():,}')
    print(f'[{year}]  Monto total UTM: {df["monto_utm"].sum():,.0f}')
    print(f'[{year}]  Universidades: {df["universidad_canon"].nunique()}')

if __name__ == '__main__':
    year = sys.argv[1]
    if year == '2021':
        rows = parse_xlsx_2021(RAW / 'wb_2021.xlsx')
    else:
        rows = parse_pdf_historical(int(year), RAW / f'wb_{year}.pdf')
    save(rows, year)
