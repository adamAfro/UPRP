import pandas, re, yaml, numpy
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
def statunit(geo:pandas.DataFrame, dist:pandas.DataFrame, coords:list[str], rads=[]):

  X = geo
  D = dist

  X = X.dropna(subset=coords)

  i0 = X.index
  X = X.reset_index()

  I = [g for g in D.columns if g in X[coords].values]
  D = D.loc[I, I].sort_index()

  N = X.value_counts(subset=coords).sort_index()
  X = X.set_index(coords)

  M = int(numpy.ceil(D.max().max()))
  for r in rads+[M]:

    R = D.copy()

   #poprawka na liczności
    L = (R <= r).astype(int)
    L = L.apply(lambda v: v.rename(None)*N, axis=1)
    L = L.sort_index()

   #ważenie przez liczność
    R = R * L

   #średnia dla danej liczności
    R = R.sum(axis=1) / L.sum(axis=1)
    if r == M: r = ''
    X = X.join(R.rename(f'meandist{r}').astype(float))

  X = X.reset_index().set_index(i0)

  return X

flow = dict()

flow['Geoportal'] = dict()
flow['Geoportal']['parse'] = GMLParse(path='geoportal.gov.pl/wfs/name.gml').map('geoportal.gov.pl/wfs/name.pkl')

flow['Misc'] = dict()
flow['Misc']['geodata'] = GeoXLSXload(path='prom/df_adresses_with_coordinates.xlsx').map('prom/df_adresses_with_coordinates.pkl')
flow['Misc']['dist'] = distcalc(flow['Misc']['geodata'], coords=['lat', 'lon']).map('prom/dists.pkl')