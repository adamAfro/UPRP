import pandas, numpy
from lib.flow import Flow

@Flow.From()
def applGUS(path:str):

  T = pandas.read_excel(path, sheet_name='DANE')

  T = T.replace('-', numpy.nan)
  T = T.dropna(subset='Wartosc')
  T = T.rename(columns={ 'Kod': 'id', 'Wynalazki': 'event', 'Wartosc': 'count', 'Rok': 'year', 'Nazwa': 'name' })
  T['event'] = T['event'].replace({ 'zgłoszenia w UPRP - ogółem': 'application' })
  T = T[ T['event'] == 'application' ]
  T = T[['event', 'count', 'year']]
  T['count'] = T['count'].astype(int)

  return T


flow = { 'GUS': { 'UPRP' : applGUS(path='GUS/UPRP-pl.xlsx') } }