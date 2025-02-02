import pandas, re, numpy, geopandas as gpd
import xml.etree.ElementTree as ET
from pyproj import Transformer
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
    L['gid'] = L['identyfikator miejscowości z krajowego rejestru urzędowego podziału terytorialnego kraju teryt']
    L = L[[ 'gid', 'rodzaj', 'nazwa miejscowości', 'powiat (miasto na prawach powiatu)', 'gmina', 'województwo', 'latitude', 'longitude' ]]
    L.columns = ['gid', 'type', 'city', 'gmina', 'powiat', 'województwo', 'lat', 'lon']

    return L

@Flow.From()
def distcalc(cities:pandas.DataFrame, coords:list[str]):

  X = cities

  X = X.dropna(subset=coords)
  X = X[ X['type'] == 'miasto' ]
  X = X.set_index(coords, drop=False)
  Y = distmx(X, coords[1], coords[0])

  return Y

#https://gis-support.pl/baza-wiedzy-2/dane-do-pobrania/granice-administracyjne/
@Flow.From()
def gisload(path:str):

  names = {
    'geometry': 'geometry',
    'JPT_KOD_JE': 'gid',
    'JPT_NAZWA_': 'name',
    'RODZAJ': 'type'
  }

  X = gpd.read_file(path).to_crs(epsg=4326).rename(columns=names)
  X = X[[k for k in names.values()]]

  return X

geoportal = GMLParse(path='geoportal.gov.pl/wfs/name.gml').map('geoportal.gov.pl/wfs/name.pkl')
geodata = GeoXLSXload(path='prom/df_adresses_with_coordinates.xlsx').map('prom/df_adresses_with_coordinates.pkl')
dist = distcalc(geodata, coords=['lat', 'lon']).map('prom/dists.pkl')

region = [gisload(path='map/powiaty.shp').map('map/polska.pkl'),
          gisload(path='map/wojewodztwa.shp').map('map/wojewodztwa.pkl'),
          gisload(path='map/polska.shp').map('map/powiaty.pkl')]