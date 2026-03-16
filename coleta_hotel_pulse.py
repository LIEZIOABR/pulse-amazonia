import os
import requests
from datetime import date

SERPAPI_KEY = os.environ['SERPAPI_KEY']
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']

HOTEIS = [
    {'hotel_id': 'intercity_campinas',   'query': 'Hotel Intercity Campinas Aquidaba'},
    {'hotel_id': 'hotel_contemporaneo',  'query': 'Hotel Contemporaneo Campinas'},
    {'hotel_id': 'golden_park_campinas', 'query': 'Golden Park Campinas'},
    {'hotel_id': 'monreale_express',     'query': 'Monreale Express Campinas'},
    {'hotel_id': 'slaviero_campinas',    'query': 'Slaviero Campinas'},
]

CHECK_IN  = str(date.today())
CHECK_OUT = str(date.today().replace(day=date.today().day + 1))

def coletar_tarifa(hotel):
    params = {
        'engine': 'google_hotels',
        'q': hotel['query'],
        'gl': 'br',
        'hl': 'pt',
        'currency': 'BRL',
        'check_in_date': CHECK_IN,
        'check_out_date': CHECK_OUT,
        'adults': 1,
        'api_key': SERPAPI_KEY
    }
    r = requests.get('https://serpapi.com/search', params=params)
    data = r.json()
    props = data.get('properties', [])
    if not props:
        print(f"[AVISO] Nenhum resultado para {hotel['hotel_id']}")
        return None
    p = props[0]
    tarifa = p.get('rate_per_night', {}).get('extracted_lowest')
    token  = p.get('property_token', '')
    fonte  = p.get('prices', [{}])[0].get('source', 'Google Hotels') if p.get('prices') else 'Google Hotels'
    return {
        'hotel_id':      hotel['hotel_id'],
        'data_coleta':   CHECK_IN,
        'tarifa_minima': tarifa,
        'fonte':         fonte,
        'property_token': token
    }

def salvar_supabase(registro):
    headers = {
        'apikey': SUPABASE_KEY,
        'Authorization': f'Bearer {SUPABASE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'return=minimal'
    }
    r = requests.post(
        f'{SUPABASE_URL}/rest/v1/hotel_pulse_tarifas',
        json=registro,
        headers=headers
    )
    print(f"[{registro['hotel_id']}] status={r.status_code} tarifa={registro['tarifa_minima']}")

for hotel in HOTEIS:
    reg = coletar_tarifa(hotel)
    if reg:
        salvar_supabase(reg)
