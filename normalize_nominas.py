"""Normaliza nombres de universidades y consolida todas las nóminas en un solo parquet."""
import pandas as pd
import re
from pathlib import Path

NOM = Path('data/nominas')
OUT = Path('data/nominas_consolidado.parquet')

# Canonical name map (clave = variantes normalizadas upper; valor = nombre canónico)
CANON = {
    # Formato largo (2022+)
    'U.DE CHILE':                          'U. de Chile',
    'UCHILE':                              'U. de Chile',
    'PONTIFICIA U. CATOLICA DE CHILE':     'PUC de Chile',
    'PUC':                                 'PUC de Chile',
    'PONTIFICIA U. CATOLICA DE VALPARAISO':'PUC de Valparaíso',
    'U.DE CONCEPCION':                     'U. de Concepción',
    'U.DE SANTIAGO':                       'U. de Santiago',
    'U.FEDERICO SANTA MARIA':              'UTFSM',
    'U.AUSTRAL DE CHILE':                  'U. Austral',
    'U.CATOLICA DEL NORTE':                'U. Católica del Norte',
    'U.CATOLICA DE LA SS.CONCEPCION':      'U. Católica Santísima Concepción',
    'U.CATOLICA DE TEMUCO':                'U. Católica de Temuco',
    'U.CATOLICA DEL MAULE':                'U. Católica del Maule',
    'U.DE VALPARAISO':                     'U. de Valparaíso',
    'U.DE ANTOFAGASTA':                    'U. de Antofagasta',
    'U.DE ATACAMA':                        'U. de Atacama',
    'U.DE LA FRONTERA':                    'U. de La Frontera',
    'U.DE LA SERENA':                      'U. de La Serena',
    'U.DE MAGALLANES':                     'U. de Magallanes',
    'U.DE PLAYA ANCHA':                    'U. de Playa Ancha',
    'U.DE TALCA':                          'U. de Talca',
    'U.DE TARAPACA':                       'U. de Tarapacá',
    'U.DEL BIO-BIO':                       'U. del Bío-Bío',
    'U.METROP. DE CS. EDUCACION':          'UMCE',
    'U.TECNOLOGICA METROPOLITANA':         'UTEM',
    'U.ARTURO PRAT':                       'U. Arturo Prat',
    'U. DE LOS LAGOS':                     'U. de Los Lagos',
    "U. O'HIGGINS":                        "U. de O'Higgins",
    "UNIVERSIDAD DE O'HIGGINS":            "U. de O'Higgins",
    "UNIVERSIDAD DE O HIGGINS":            "U. de O'Higgins",
    # Códigos cortos (2015-2017 + 2021)
    'USACH':                               'U. de Santiago',
    'UDEC':                                'U. de Concepción',
    'UCV':                                 'PUC de Valparaíso',
    'UTFSM':                               'UTFSM',
    'UACH':                                'U. Austral',
    'UCN':                                 'U. Católica del Norte',
    'UCSC':                                'U. Católica Santísima Concepción',
    'UCT':                                 'U. Católica de Temuco',
    'UCM':                                 'U. Católica del Maule',
    'UCMAULE':                             'U. Católica del Maule',
    'UV':                                  'U. de Valparaíso',
    'UVALPO':                              'U. de Valparaíso',
    'UANTOF':                              'U. de Antofagasta',
    'UDA':                                 'U. de Atacama',
    'UATACAMA':                            'U. de Atacama',
    'UFRO':                                'U. de La Frontera',
    'USERENA':                             'U. de La Serena',
    'UMAG':                                'U. de Magallanes',
    'UPLA':                                'U. de Playa Ancha',
    'UTALCA':                              'U. de Talca',
    'UTA':                                 'U. de Tarapacá',
    'UBIOBIO':                             'U. del Bío-Bío',
    'UBB':                                 'U. del Bío-Bío',
    'UMCE':                                'UMCE',
    'UTEM':                                'UTEM',
    'UNAP':                                'U. Arturo Prat',
    'ULAGOS':                              'U. de Los Lagos',
    'UOH':                                 "U. de O'Higgins",
}

def normalize(u):
    s = str(u).strip()
    for k, v in CANON.items():
        if s.upper() == k.upper():
            return v
    return s

def calc_dv(rut):
    """Calcula dígito verificador chileno mod-11."""
    try:
        rut = str(rut).strip()
        mul = [2, 3, 4, 5, 6, 7]
        s = sum(int(d) * mul[i % 6] for i, d in enumerate(reversed(rut)))
        r = 11 - (s % 11)
        return '0' if r == 11 else ('K' if r == 10 else str(r))
    except Exception:
        return ''

def main():
    dfs = []
    for f in sorted(NOM.glob('*.parquet')):
        df = pd.read_parquet(f)
        df['universidad_canon'] = df['universidad'].apply(normalize)
        # Recalcular DV (los PDFs 2016-2017 tienen DVs corruptos por extracción PDF)
        df['dv'] = df['rut'].apply(calc_dv)
        df['rut_dv'] = df['rut'].astype(str) + '-' + df['dv']
        desconocidas = df[df['universidad_canon'] == df['universidad']]['universidad'].unique().tolist()
        # si hay nombres NO mapeados, los reportamos
        stranger = [u for u in desconocidas if u.upper() not in CANON]
        if stranger: print(f"[{f.stem}] sin canonicalizar:", stranger)
        dfs.append(df)
    full = pd.concat(dfs, ignore_index=True)
    full.to_parquet(OUT, index=False)
    print(f"\nEscrito {OUT}: {len(full):,} filas totales")
    print(f"Años: {sorted(full['year'].unique())}")
    print(f"Universidades canónicas: {full['universidad_canon'].nunique()}")

    # evolución por año
    print("\n=== Evolución por año ===")
    summ = full.groupby('year').agg(
        deudores=('rut','nunique'),
        filas=('rut','size'),
        utm_total=('monto_utm','sum'),
        utm_avg=('monto_utm','mean'),
        utm_med=('monto_utm','median'),
        universidades=('universidad_canon','nunique'),
    ).round(1)
    print(summ.to_string())

    # evolución por universidad (top 10 más grandes)
    print("\n=== Top 10 universidades — evolución deudores ===")
    pivot_n = full.groupby(['year','universidad_canon']).size().unstack('year').fillna(0).astype(int)
    pivot_n['total'] = pivot_n.sum(axis=1)
    print(pivot_n.sort_values('total', ascending=False).head(10).to_string())

if __name__ == '__main__':
    main()
