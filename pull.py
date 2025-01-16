import sys, pandas, yaml, json, re, os, asyncio, aiohttp, unicodedata, zipfile
import xml.etree.ElementTree as ET
from lib.storage import Storage
from lib.geo import closest
from lib.name import distinguish, classify
from pyproj import Transformer
from lib.flow import Flow

@Flow.From()
def Fetch(queries:pandas.Series, URL:str, outdir:str):

  "Pobieranie pełnych stron HTML z wynikami wyszukiwania."

  async def scrap(P:list):

    t = 1
    H = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    async with aiohttp.ClientSession(headers=H) as S:
      for i, p in P.iterrows():
        j, d = p['country'].upper(), ''.join(re.findall(r'\d+', p['number']))
        o = f"{outdir}/{j}{d}.html"
        if os.path.exists(o): continue
        x = f"{URL}/{j}{d}"
        async with S.get(x) as y0:
          y = await y0.text()
          await asyncio.sleep(t)
          with open(o, "w") as f: f.write(y)

  _, P = queries
  asyncio.run(scrap(P))

def txtnorm(input_str):
  nfkd_form = unicodedata.normalize('NFKD', input_str)
  return ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

@Flow.From()
def LoadOSM(path:str):

  with open(path) as f:
    X = json.load(f)

  Y = []
  for H in X['features']:
    try: k = H['properties']['name']
    except KeyError: continue
    c = H['geometry']['coordinates']
    Y.append({'name': re.sub(r'\W+', ' ', k).upper().strip(), 
              'lat': c[1], 'lon': c[0]})

  Y = pandas.DataFrame(Y)

  Y['norm'] = Y['name'].apply(txtnorm)

  return Y

@Flow.From()
def Nameread(asnstores:dict[Storage, str]):

  """
  Odróżnia w danych imiona, nazwiska i nazwy organizacji.
  Zwraca zbiór słów imienniczych (imię albo nazwisko), 
  samych imion, samych nazwisk, nazw organizacji.
  Nazwy bez jednoznacznej klasyfikacji są pomijane.
  """

  K0 = ['name', 'fname', 'lname', 'city']
  P = pandas.DataFrame()
  T = pandas.DataFrame()

  for assignpath, S in asnstores.items():

    with open(assignpath, 'r') as f:
      S.assignement = yaml.load(f, Loader=yaml.FullLoader)

    T = pandas.concat([T, S.melt('type-name').reset_index()])

    for h in ['assignee', 'applicant', 'inventor']:

      K = [f'{k0}-{h}' for k0 in K0]
      p = pandas.concat([S.melt(f'{k}').reset_index() for k in K] + [T])
      p = p[['id', 'doc', 'value', 'assignement']]
      p['value'] = p['value'].str.upper().str.replace(r'\s+', ' ', regex=True).str.strip()
      p = p.pivot_table(index=['id', 'doc'], columns='assignement',
                        values='value', aggfunc='first')

      p.columns = [k.split('-')[0] for k in p.columns]
      p['role'] = h

      P = pandas.concat([P, p]) if not P.empty else p

  P = P.reset_index(drop=True).drop_duplicates()

  return distinguish(P, valuekey='name', kindkey='kind', normkey='norm',
                     firstname='fname', lastname='lname', commonname='flname',
                     orgname='org',
                     orgqueries=['type == "LEGAL"'],
                     namequeries=['role == "inventor"'],
                     orgkeysubstr=['&', 'INTERNAZIO', 'INTERNATIO', 'INC.', 'ING.', 'SP. Z O. O.'],
                     orgkeywords=[x for X in ['COMPANY PRZEDSIĘBIORSTWO PRZEDSIEBIORSTWO FUNDACJA INSTYTUT INSTITUTE',
                                              'HOSPITAL SZPITAL'
                                              'COMPANY LTD SPÓŁKA LIMITED GMBH ZAKŁAD PPHU',
                                              'KOPALNIA SPÓŁDZIELNIA SPOLDZIELNIA FABRYKA',
                                              'ENTERPRISE TECHNOLOGY',
                                              'LLC CORPORATION INC',
                                              'MIASTO GMINA URZĄD',
                                              'GOVERNMENT RZĄD',
                                              'AKTIENGESELLSCHAFT KOMMANDITGESELLSCHAFT',
                                              'UNIWERSYTET UNIVERSITY AKADEMIA ACADEMY',
                                              'POLITECHNIKA'] for x in X.split()])

@Flow.From()
def Personify(storage:Storage, assignpath:str, nameset:pandas.DataFrame):

  #TODO: zamiana imion z fname i lname na flname jeśli taka jest sytuacja
  #BUG: pojawiają się duplikaty z jakiegoś powodu, napewno z tego powyżej ale czy to jedyne źródło? 

  """
  Zwraca ramkę z danymi osobowymi.

  Identyfikuje imiona osób na podstawie słownika imion o ile
  każde słowo w nazwie dłusze niż 2 znaki jest w słowniku.

  Działa tylko na wewnętrnzym zbiorze - do porpawnia

  Normalizacja nie jest jeszcze poprawnie zaimplementowana
  """

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  K0 = ['name', 'fname', 'lname', 'city']
  P = pandas.DataFrame()

  for h in ['assignee', 'applicant', 'inventor']:

    K = [f'{k0}-{h}' for k0 in K0]
    p = pandas.concat([S.melt(f'{k}').reset_index() for k in K])
    p = p[['id', 'doc', 'value', 'assignement']]
    p['value'] = p['value'].str.upper().str.replace(r'\s+', ' ', regex=True).str.strip()
    p = p.pivot_table(index=['id', 'doc'], columns='assignement',
                      values='value', aggfunc='first')

    p.columns = [k.split('-')[0] for k in p.columns]
    p['role'] = h

    P = pandas.concat([P, p]) if not P.empty else p

  if P.empty:
    return pandas.DataFrame(), pandas.DataFrame()

  return classify(P, nameset, valuekey='name', kindkey='kind', normkey='norm',
                  firstname='fname', lastname='lname', commonname='flname',
                  orgname='org', idkey='id', dockey='doc', rolekey='role', 
                  indexkey='index', evalkey='evalname', keyskept=['city'] if 'city' in P.columns else [])

@Flow.From()
def Geoloc(storage:Storage, geodata:pandas.DataFrame, assignpath:str):

  """
  Dopasowanie patentu do punktów geograficznych `(lat, lon)`.

  Uwagi:

  1. patent ma wiele lokalizacji (inaczej punktów powiązanych);
  2. nazwy mogą być zduplikowane; do zaimplementowania: estymacja 
  poprawnego miasta przez minimalizację średniej odległości 
  do pt.ów powiązanych.
  """

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  C = S.melt('city').drop(columns=['repo', 'id', 'col', 'frame', 'assignement'])
  C = C.drop_duplicates(subset=['doc', 'value'])
  C = C.set_index('doc')
  C = C['value'].str.split(',').explode()
  C = C.str.split(';').explode()
  C = C.str.upper().str.replace(r'\W+', ' ', regex=True)
  C = C.str.extractall(r'((?:[^\d\W]|\s)+)')[0].rename('value').dropna()
  C = C.str.upper().str.strip()
  C = C.reset_index().drop(columns='match')
  C = C.set_index('value')

  L = geodata
  L = L[ L['type'] == 'miasto' ].copy()
  L['name'] = L['name'].str.upper()\
                    .str.replace(r'\W+', ' ', regex=True)\
                    .str.strip()

  L = L.set_index('name')

  J = C.join(L, how='inner')
  J = J.reset_index().dropna(subset=['lat','lon']).dropna(axis=1)

  J = J[['doc', 'value', 'gmina', 'powiat', 'województwo', 'lat', 'lon']]
  J.columns = ['doc', 'name', 'gmina', 'powiat', 'województwo', 'lat', 'lon']
  J = J.drop_duplicates(subset=['doc', 'name', 'lat', 'lon'])

  J['lat'] = pandas.to_numeric(J['lat'])
  J['lon'] = pandas.to_numeric(J['lon'])
  Y = closest(J, 'doc', 'name', 'lat', 'lon')

  C = C[ ~ C.index.isin(Y['name']) ]
  C.index = C.index.to_series().apply(txtnorm).values
  L.index = L.index.to_series().apply(txtnorm).values

  J = C.join(L, how='inner')
  J = J.reset_index().dropna(subset=['lat','lon']).dropna(axis=1)

  J = J[['doc', 'index', 'gmina', 'powiat', 'województwo', 'lat', 'lon']]
                #^WTF: z jakiegoś powodu nie 'value'

  J.columns = ['doc', 'name', 'gmina', 'powiat', 'województwo', 'lat', 'lon']
  J = J.drop_duplicates(subset=['doc', 'name', 'lat', 'lon'])

  J['lat'] = pandas.to_numeric(J['lat'])
  J['lon'] = pandas.to_numeric(J['lon'])
  Y = pandas.concat([Y, closest(J, 'doc', 'name', 'lat', 'lon')])

  return Y

@Flow.From()
def Timeloc(storage:Storage, assignpath:str):

  "Wybiera najwcześniejsze daty dla każdego patentu"

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  C = pandas.concat([S.melt(f'date-{k}')[['doc', 'value', 'assignement']] 
                     for k in ['fill', 'application', 'exhibition', 'grant', 
                               'nogrant', 'decision', 'regional', 'priority', 'publication']])

  C['assignement'] = C['assignement'].str.split('-').str[1]
  C['value'] = pandas.to_datetime(C['value'], 
                                  errors='coerce', 
                                  format='mixed', 
                                  dayfirst=False)

  C = C.dropna(subset=['value'])
  C = C.drop_duplicates(subset=['doc', 'value', 'assignement'])
  C = C.set_index('doc')
  C = C.sort_values(by='value')

  C['year'] = C['value'].dt.year.astype(str)
  C['month'] = C['value'].dt.month.astype(str)
  C['day'] = C['value'].dt.day.astype(str)
  C['delay'] = (C['value'] - C['value'].min()).dt.days.astype(int)
  C = C.drop(columns='value')

  return C

@Flow.From()
def Classify(storage:Storage, assignpath:str):

  "Zwraca ramkę z klasyfikacjami."

  H = storage
  a = assignpath

  with open(a, 'r') as f:
    H.assignement = yaml.load(f, Loader=yaml.FullLoader)

  K = ['IPC', 'IPCR', 'CPC', 'NPC']
  K0 = ['section', 'class', 'subclass', 'group', 'subgroup']

  U = [H.melt(k).reset_index() for k in K]
  U = [m for m in U if not m.empty]
  if not U: return pandas.DataFrame()
  C = pandas.concat(U)

  C['value'] = C['value'].str.replace(r'\s+', ' ', regex=True)
  C['section'] = C['value'].str.extract(r'^(\w)') 
  C['class'] = C['value'].str.extract(r'^\w\s?(\d+)')
  C['subclass'] = C['value'].str.extract(r'^\w\s?\d+\s?(\w)')
  C['group'] = C['value'].str.extract(r'^\w\s?\d+\s?\w\s?(\d+)')
  C['subgroup'] = C['value'].str.extract(r'^\w\s?\d+\s?\w\s?\d+\s?/\s?(\d+)')
  C = C[['id', 'doc', 'assignement'] + K0]

  F = pandas.concat([H.melt(f'{k}-{k0}').reset_index() for k in K for k0 in K0])
  F = F.rename(columns={'assignement': 'classification'})
  F['assignement'] = F['classification'].str.split('-').str[0]
  P = F.pivot_table(index=['id', 'doc', 'assignement'], columns='classification', values='value', aggfunc='first').reset_index()
  P.columns = [k.split('-')[1] if '-' in k else k for k in P.columns]

  if (not C.empty) and (not P.empty):
    Y = pandas.concat([C, P], axis=0)
  elif not C.empty:
    Y = C
  elif not P.empty:
    Y = P
  else:
    return pandas.DataFrame()

  Y = pandas.concat([C, P], axis=0)
  Y.columns = ['id', 'doc', 'classification'] + K0
  Y = Y.set_index(['id', 'doc'])

  return Y

@Flow.From()
def Codepull(storage:Storage, assignpath:str):

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  K = ['frame', 'doc', 'id']
  A = S.melt('number-application').set_index(K)['value'].rename('number')
  B = S.melt('country-application').set_index(K)['value'].rename('country')
  Y = A.to_frame().join(B)

  Z = S.melt('application').set_index(K)['value'].rename('whole')
  Z = Z.str.replace(r'\W+', '', regex=True).str.upper().str.strip()
  Z = Z.str.extract(r'(?P<country>\D+)(?P<number>\d+)')

  Q = [X for X in [Y, Z] if not X.empty]
  if not Q: return pandas.DataFrame()
  Y = pandas.concat(Q, axis=0)
  Y = Y.reset_index().set_index('doc')[['country', 'number']]

  return Y

def Pull(storage:Storage, assignpath:str, 
         geodata:pandas.DataFrame, 
         nameset:pandas.DataFrame, 
         workdir:str):

  "Wyciąga dane zgodnie z przypisanymi rolami."

  os.makedirs(workdir, exist_ok=True)

  Z = Codepull(storage, assignpath).map(f'{workdir}/pat.pkl')
  G = Geoloc(storage, geodata, assignpath).map(f'{workdir}/geo.pkl')
  T = Timeloc(storage, assignpath).map(f'{workdir}/time.pkl')
  C = Classify(storage, assignpath).map(f'{workdir}/clsf.pkl')
  P = Personify(storage, assignpath, nameset).map(f'{workdir}/people.pkl')

  return Flow(callback=lambda *z: z, args=[Z, G, T, C, P])

@Flow.From()
def GMLParse(path:str):

    tree = ET.parse(path)
    root = tree.getroot()

    N = { 'ms': 'http://mapserver.gis.umn.edu/mapserver',
          'gml': 'http://www.opengis.net/gml/3.2',
          'wfs': 'http://www.opengis.net/wfs/2.0' }

    Y = []
    for M in root.findall('wfs:member', N):

      E = {}

      for e in M.find('ms:M1_UrzedoweNazwyMiejscowosci', N):
        E[e.tag.split('}')[1]] = e.text

      P = M.find('ms:M1_UrzedoweNazwyMiejscowosci/ms:msGeometry/gml:Point', N)
      U = P.find('gml:pos', N)
      if U is None: continue
      E['latitude'], E['longitude'] = U.text.split()
      E['srsName'] = P.attrib['srsName']

      Y.append(E)

    L = pandas.DataFrame(Y)
    T = Transformer.from_crs('EPSG:2180', 'EPSG:4326', always_xy=True)
    L['longitude'], L['latitude'] = zip(*L.apply(lambda r: T.transform(r['latitude'], r['longitude']), axis=1))

    L = L[[ 'RODZAJOBIEKTU', 'NAZWAGLOWNA', 'GMINA', 'POWIAT', 'WOJEWODZTWO', 'latitude', 'longitude' ]]
    L.columns = ['type', 'name', 'gmina', 'powiat', 'województwo', 'lat', 'lon']

    return L

@Flow.From()
def GeoXLSXload(path:str):

    L = pandas.read_excel(path, engine='openpyxl')
    L.columns = [re.sub(r'\s+', ' ', c).lower() for c in L.columns]
    L = L[[ 'rodzaj', 'nazwa miejscowości', 'powiat (miasto na prawach powiatu)', 'gmina', 'województwo', 'latitude', 'longitude' ]]
    L.columns = ['type', 'name', 'gmina', 'powiat', 'województwo', 'lat', 'lon']

    return L


from dirs import data as D
from profiling import flow as f0

flow = { k: dict() for k in D.keys() }

flow['Geoportal'] = dict()
flow['Geoportal']['parse'] = GMLParse(path='geoportal.gov.pl/wfs/name.gml').map('geoportal.gov.pl/wfs/name.pkl')

flow['geodata'] = GeoXLSXload(path='prom/df_adresses_with_coordinates.xlsx').map('prom/df_adresses_with_coordinates.pkl')

flow['nameread'] = Nameread({ 
  D['Google']+'/assignement.yaml': f0['Google']['profiling'],
  D['Lens']+'/assignement.yaml': f0['Lens']['profiling'],
  D['UPRP']+'/assignement.yaml': f0['UPRP']['profiling'] 
}, outpath='names.pkl')

for k, p in D.items():

  flow[k]['pull'] = Pull(f0[k]['profiling'], assignpath=p+'/assignement.yaml', 
                         geodata=flow['geodata'],
                         nameset=flow['nameread'],
                         workdir=p+'/bundle')

for k0 in ['Lens', 'Google']:

  k = f'UPRP-{k0}'
  flow[k] = dict()
  p = f'{D["UPRP"]}/{D[k0]}'
  D[k] = p

  flow[k] = dict()

  flow[k]['pull'] = Pull(f0['UPRP']['profiling'], assignpath=D['UPRP']+'/assignement.yaml', 
                         geodata=flow['geodata'],
                         nameset=flow['nameread'],
                         workdir=p+'/bundle')