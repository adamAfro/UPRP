import pandas, numpy
from lib.flow import Flow

@Flow.From()
def applGUS(path:str):

  T = pandas.read_excel(path, sheet_name='DANE')

  T = T.replace('-', numpy.nan)
  T = T.dropna(subset='Wartosc')
  T = T.rename(columns={ 'Kod': 'id', 'Wynalazki': 'event', 'Wartosc': 'count', 'Rok': 'year', 'Nazwa': 'name' })
  T['event'] = T['event'].replace({ 'zgłoszenia w UPRP - ogółem': 'application',
                                    'zgłoszenie w UPRP - jednostki naukowe Polskiej Akademii Nauk, instytuty badawcze, szkoły wyższe': 'application-org',
                                    'zgłoszenie w UPRP - podmioty gospodarcze': 'application-org',
                                    'zgłoszenie w UPRP - osoby fizyczne': 'application-people' })

  T['organisation'] = T['event'].str.endswith('-org')
  T['event'] = 'application'

  T = T[['event', 'count', 'year', 'organisation']]
  T['count'] = T['count'].astype(int)

  T = T.loc[T.index.repeat(T['count'])].reset_index(drop=True)
  T = T.drop(columns=['count'])

  return T


flow = { 'GUS': { 'UPRP' : applGUS(path='GUS/UPRP-pl.xlsx') } }