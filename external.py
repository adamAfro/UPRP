import pandas, numpy, requests
from lib.flow import Flow
from time import sleep

class BDL:

  headers = {'X-ClientId': 'b0a51e89-3757-477c-726a-08dd3c85dea7'}

  @Flow.From()
  @staticmethod
  def ls():

    #TODO paginate
    I = BDL._supcatg()
    X = [v for k in I for v in BDL._catg(k)]
    Y = [v for x in X for v in BDL._catgvars(x['id'])]

    return Y

  @staticmethod
  def _catgvars(id:str):

    r = requests.get(f'https://bdl.stat.gov.pl/api/v1/variables?subject-id={id}', headers=BDL.headers)
    x = r.json()
    y = x['results']

    return y

  @staticmethod
  def _supcatg():

    r = requests.get('https://bdl.stat.gov.pl/api/v1/subjects', headers=BDL.headers)
    x = r.json()
    y = [v['id'] for v in x['results']]

    return y

  @staticmethod
  def _catg(id:str):

    r = requests.get(f'https://bdl.stat.gov.pl/api/v1/subjects?parent-id={id}', headers=BDL.headers)
    x = r.json()
    if not x['results']:
      return []

    y = [v for v in x['results'] if v['hasVariables']]
    q = [v for v in x['results'] if not v['hasVariables']]
    for v in q:
      try:
        sleep(1)
        y.extend(BDL._catg(v['id']))
      except Exception as e:
        raise Exception(f"Error processing id {v['id']}: {str(e)}")

    return y

  @Flow.From()
  @staticmethod
  def pull(id:str, years:list):

    ystr = '&'.join([f'year={v}' for v in years])
    #TODO paginate
    r = requests.get(f'https://bdl.stat.gov.pl/api/v1/data/by-variable/{id}?format=jsonapi&{ystr}', headers=BDL.headers)
    x = r.json()
    X = [pandas.DataFrame(V['attributes']['values']).assign(name=V['attributes']['name'], id=V['id']) for V in x['data']]
    Y = pandas.concat(X)

    return Y

@Flow.From()
def applBDL(path:str):

  T = pandas.read_excel(path, sheet_name='DANE')

  T = T.replace('-', numpy.nan)
  T = T.dropna(subset='Wartosc')
  T = T.rename(columns={ 'Kod': 'id', 'Wynalazki': 'event', 'Wartosc': 'count', 'Rok': 'year', 'Nazwa': 'name' })
  T['event'] = T['event'].replace({ 'zgłoszenia w UPRP - ogółem': 'application' })
  T = T[ T['event'] == 'application' ]
  T = T[['event', 'count', 'year']]
  T['count'] = T['count'].astype(int)

  return T


GUSls = BDL.ls().map('GUS/BDL/ls.pkl')

flow = { 'GUS': { 'UPRP' : applBDL(path='GUS/BDL/UPRP-pl.xlsx'),
                  'ls' : GUSls } }