import pandas, re, yaml
import xml.etree.ElementTree as ET
from pyproj import Transformer
from lib.storage import Storage
from lib.geo import closest
from lib.flow import Flow
from lib.geo import distmx

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
    L.columns = ['type', 'city', 'gmina', 'powiat', 'województwo', 'lat', 'lon']

    return L

@Flow.From()
def GeoXLSXload(path:str):

    L = pandas.read_excel(path, engine='openpyxl')
    L.columns = [re.sub(r'\s+', ' ', c).lower() for c in L.columns]
    L = L[[ 'rodzaj', 'nazwa miejscowości', 'powiat (miasto na prawach powiatu)', 'gmina', 'województwo', 'latitude', 'longitude' ]]
    L.columns = ['type', 'city', 'gmina', 'powiat', 'województwo', 'lat', 'lon']

    return L

@Flow.From()
def distcalc(cities:pandas.DataFrame, coords:list[str]):

  X = cities

  X = X.dropna(subset=coords)
  X = X[ X['type'] == 'miasto' ]
  X = X.set_index(coords, drop=False)
  Y = distmx(X, coords[1], coords[0])

  return Y

@Flow.From()
def stats(geo:pandas.DataFrame, dist:pandas.DataFrame, coords:list[str]):

  import tqdm

  X = geo
  D = dist

  X = X.dropna(subset=coords)
  N = X.value_counts(subset=coords)
  n = N.sum()
  D = D.loc[:, [ g for g in D.columns if g in X[coords].values ]]

  for g in D.columns:
    D.loc[:, g] = D.loc[:, g] * N.loc[g]

  X['meandist'] = 0.0
  for i, x in tqdm.tqdm(X.iterrows(), total=n):
    d = D.loc[(x['lat'], x['lon'])]
    if isinstance(d, pandas.DataFrame):
      raise Exception('Multiple values')
    X.loc[i, 'meandist'] = d.sum()/n

  return X

flow = dict()

flow['Geoportal'] = dict()
flow['Geoportal']['parse'] = GMLParse(path='geoportal.gov.pl/wfs/name.gml').map('geoportal.gov.pl/wfs/name.pkl')

flow['Misc'] = dict()
flow['Misc']['geodata'] = GeoXLSXload(path='prom/df_adresses_with_coordinates.xlsx').map('prom/df_adresses_with_coordinates.pkl')
flow['Misc']['dist'] = distcalc(flow['Misc']['geodata'], coords=['lat', 'lon']).map('prom/dists.pkl')