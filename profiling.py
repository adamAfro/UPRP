import yaml, re
from lib.storage import Storage
from lib.profile import Profiler
from lib.alias import simplify
from lib.flow import Flow

@Flow.From()
def Profiling(dir:str, kind:str, assignpath:str, aliaspath:str,  profargs:dict={}):

  """
  Profilowanie danych, z różnych źródeł, do relacyjnych ramek danych.

  Profilowanie składa się z 2 etapów parsowania danych, 
  etapu normalizacji nazw oraz etapu manualnego przypisywania ról
  dla kolumn w wytworzonych ramkach danych.

  Dane wejściowe to heterogeniczna struktura 
  zagnieżdżonych obiektów z parametrami, które mogą być zarówno
  obiektami, listami obiektów oraz wartościami skalarnymi.

  To jak zagnieżdżony jest obiekt jest tutaj nazwane ścieżką,
  przykładowo dla obiektu `"krzesła"` w stukturze 
  `{ "dom": "pokój": { "krzesła": 3 } }`, ścieżka to "dom/pokój/krzesła".

  ***

  Heterogeniczność odnosi się do kilku faktów na temat danych:

  - istnienie parametru dla danej obserwacji nie jest gwarantowane;
  - typ wartości może różnić się po między obserwacjami mimo identycznej ścieżki;
  - to samo rzeczywiste zjawisko (np. autorstwo patentu) może być reprezentowane 
  w różny sposób: z różnymi parametrami, o różnych ścieżkach.

  Różnice w danych wynikają z różnic w wersjach schematu odpowiedniego 
  dla danego okresu, albo z braków danych.

  ***

  **Parsowanie danych** polega na odczytaniu zawartości zadanych plików,
  zgodnie z ich formatowaniem. Dane przetworzone na strukturę słowników
  są dalej analizowane pod kątem posiadania list podobnych obiektów.

  Kolejnym etapem jest tworzenie struktury homogenicznejz możliwymi brakami.
  Obiekty w listach są przetwarzane na oddzielne encje w oddzielnej strukturze,
  z relacją do obiektu w którym się znajdują. Wszystkie pozostałe wartości są
  przypisywane bezpośrednio do obiektu, w którym się znajdują niezależnie od
  poziomu zagnieżdżenia.

  W obu etapach nazwami danych i encji są ich ścieżki. Są one mało czytelne,
  z powodu ich długości dlatego wymagają normalizacji.

  ***

  **Normalizacja nazw** polega na przypisaniu krótkich, czytelnych nazw dla
  ścieżek w danych. Ścieżki zostają podzielone na pojedyncze fragmenty,
  czyli kolejne nazwy obiektów w których się znajdują. Z takich ciągów
  tworzony jest graf drzewa. W iteracjach po wierzchołkach wyciągane są
  nazwy w sposób, który zapewnia ich krótkość i unikalność. Jeśli pierwsza
  nazwa nie jest unikalna dodawany jest kolejny wierzchołek od końca, aż
  zapewni to unikalność. Jeśli cały proces nie odniesie sukcesu dodawane
  są liczby, aby zapewnić unikalność.

  ***

  **Przypisanie ról** polega na ręcznym przypisaniu nazw kolumnom w ramkach.
  Te nazwy są używane na dalszych etapach wyciągania danych.
  """

  P = Profiler( **profargs )
  
  kind = kind.upper()
  assert kind in ['JSON', 'JSONL', 'XML', 'HTMLMICRODATA']

  if kind == 'XML':
    P.XML(dir)
  elif kind == 'HTMLMICRODATA':
    P.HTMLmicrodata(dir)
  elif kind == 'JSON':
    P.JSON(dir)
  elif kind == 'JSONL':
    P.JSONl(dir, listname="data")

  H = P.dataframes()

  def pathnorm(x:str):
    x = re.sub(r'[^\w\.\-/\_]|\d', '', x)
    x = re.sub(r'\W+', '_', x)
    return x

  L = simplify(H, norm=pathnorm)
  H = { L['frames'][h0]: X.set_index(["id", "doc"])\
        .rename(columns=L['columns'][h0]) for h0, X in H.items() if not X.empty }

  L['columns'] = { L['frames'][h]: { v: k for k, v in Q.items() }  
                    for h, Q in L['columns'].items() }
  L['frames'] = { v: k for k, v in L['frames'].items() }
  with open(aliaspath, 'w') as f:
    yaml.dump(L, f, indent=2)#do wglądu

  A = { h: { k: None for k in V.keys() } for h, V in L['columns'].items() }
  with open(assignpath, 'w') as f:
    yaml.dump(A, f, indent=2)#do ręcznej edycji

  return Storage(dir, H)

from dirs import data as D

flow = { k: dict() for k in D.keys() }

flow['UPRP']['profiling'] = Profiling(D['UPRP']+'/raw/', kind='XML',
                                      assignpath=D['UPRP']+'/assignement.null.yaml', 
                                      aliaspath=D['UPRP']+'/alias.yaml').map(D['UPRP']+'/storage.pkl')

flow['Lens']['profiling'] = Profiling(D['Lens']+'/res/', kind='JSONL',
                                      assignpath=D['Lens']+'/assignement.null.yaml', 
                                      aliaspath=D['Lens']+'/alias.yaml').map(D['Lens']+'/storage.pkl')

flow['USPG']['profiling'] = Profiling(D['USPG']+'/raw/', kind='XML',
                                      profargs=dict(only=['developer.uspto.gov/grant/raw/us-patent-grant/us-bibliographic-data-grant/']),
                                      assignpath=D['USPG']+'/assignement.null.yaml',
                                      aliaspath=D['USPG']+'/alias.yaml').map(D['USPG']+'/storage.pkl')

flow['USPA']['profiling'] = Profiling(D['USPA']+'/raw/', kind='XML',
                                      profargs=dict(only=['developer.uspto.gov/application/raw/us-patent-application/us-bibliographic-data-application/']),
                                      assignpath=D['USPA']+'/assignement.null.yaml', 
                                      aliaspath=D['USPA']+'/alias.yaml').map(D['USPA']+'/storage.pkl')

flow['Google']['profiling'] = Profiling(D['Google']+'/web/', kind='HTMLmicrodata',
                                        assignpath=D['Google']+'/assignement.null.yaml', 
                                        aliaspath=D['Google']+'/alias.yaml').map(D['Google']+'/storage.pkl')