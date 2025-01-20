import pandas, re, yaml
import xml.etree.ElementTree as ET
from pyproj import Transformer
from lib.storage import Storage
from lib.geo import closest
from lib.flow import Flow

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

flow = dict()

flow['Geoportal'] = dict()
flow['Geoportal']['parse'] = GMLParse(path='geoportal.gov.pl/wfs/name.gml').map('geoportal.gov.pl/wfs/name.pkl')

flow['Misc'] = dict()
flow['Misc']['geodata'] = GeoXLSXload(path='prom/df_adresses_with_coordinates.xlsx').map('prom/df_adresses_with_coordinates.pkl')