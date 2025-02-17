r"""
\section{Tabele dotyczące patentów}

W bazie danych istnieją 
  4 tabele
    istotne dla analizy patentów.
"""

#lib
import lib.flow, lib.geo, lib.storage, gloc, prfl
from util import strnorm, data as D

#calc
import pandas, yaml

#plot
import altair as Plot

@lib.flow.placeholder()
def Code(storage:lib.storage.Storage, assignpath:str):

  r"""
  \subsection{Metadane}
  \label{metadane}

  Metadane to informacje o patentach, 
    których zbieranie jest realizowane 
      na potrzeby wewnętrznych systemów \ac{UPRP}.
  Wśród metadanych można wyróżnić 
    informacje o numerze złożonej aplikacji patentowej oraz
    kraju, w którym został złożony.
  Niniejsza analiza 
    wykorzystuje numery patentowe 
      do ich identyfikacji.
  Każdy patent ma przydzielony 
    numer złożenia aplikacji.
    W razie otrzymania ochrony, 
      dostaje również numer przyznania patentu.
  """

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

  assert { 'country', 'number' }.issubset(Y.columns)
  assert { 'doc' }.issubset(Y.index.names)
  return Y

@lib.flow.placeholder()
def Event(storage:lib.storage.Storage, assignpath:str, codes:pandas.DataFrame):

  r"""
  \subsection{Wydarzenia związane z patentami}
  \label{wydarzenia}

  Poszczególne czynności związane z ochroną patentów 
    są rejestrowane.
  Każde wydarzenie jest powiązane 
    z konkretną datą kalendarzową.
  W bazie jest kilka typów wydarzeń 
    związanych z patentami.
  Analiza obejmuje 
    wyłącznie momenty złożenia aplikacji oraz
    uzyskania ochrony patentowej.
  """

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  X = pandas.concat([S.melt(f'date-{k}')[['doc', 'value', 'assignement']] 
                    for k in ['fill', 'application', 'exhibition', 'grant', 
                              'nogrant', 'decision', 'regional', 'priority', 'publication']])

  X['assignement'] = X['assignement'].str.split('-').str[1]
  X['value'] = pandas.to_datetime(X['value'], 
                                  errors='coerce', 
                                  format='mixed', 
                                  dayfirst=False)

  X = X.dropna(subset=['value'])
  X = X.drop_duplicates(subset=['doc', 'value', 'assignement'])
  X = X.set_index('doc')
  X = X.sort_values(by='value')

  X['year'] = X['value'].dt.year.astype(str)
  X['month'] = X['value'].dt.month.astype(str)
  X['day'] = X['value'].dt.day.astype(str)
  X['date'] = pandas.to_datetime(X[['year', 'month', 'day']])
  X['delay'] = (X['value'] - X['value'].min()).dt.days.astype(int)
  X = X.drop(columns='value')

  O = ['exhibition', 'priority', 'regional', 'fill', 'application', 'nogrant', 'grant', 'decision', 'publication']
  X['event'] = pandas.Categorical(X['assignement'], ordered=True, categories=O)
  X = X.drop(columns='assignement')

  X = X.join(codes, how='left')

  assert { 'event', 'year', 'date', 'month', 'day', 'delay', 'country', 'number' }.issubset(X.columns)
  assert { 'doc' }.issubset(X.index.names)
  return X

@lib.flow.placeholder()
def Classify(storage:lib.storage.Storage, assignpath:str, codes:pandas.DataFrame, extended=False):

  r"""
  \subsection{Klasyfikacje patentów}

  Klasyfikacje patentowe 
    to systemy, 
      które pozwalają na przypisanie patentów 
        do odpowiednich dziedzin.

  \subsubsection
  {Międzynarodowa Klasyfikacja Patentów}
  \label{IPC}

  W Polsce funkcjonuje klasyfikacja
    \ac{MKP}, 
      czyli \ac{IPC}. 

  Zapis klasyfikacji w tym systemie 
    to ciąg
      cyfrowo-literowy 
      składający się z 4 części:
      $\Lambda\ \theta_{1} \theta_{2}\ \beta\ \hat\vartheta_{1} \hat\vartheta_{2} \vartheta_{3}$,
        gdzie 
          $\Lambda$ to symbol dziedziny, 
          $\theta_{1} \theta_{2}$ to cyfry klasy,
          $\beta$ to litera podklasy,
          a $\hat\vartheta_{1} \hat\vartheta_{2} \vartheta_{3}$ to cyfry grupy.
            Grupa jest symbolizowana za pomocą 1 do 3 cyfr, 
            $\hat\vartheta_{1} \hat\vartheta_{2}$ są opcjonalne.

  Analize są poddane wyłącznie sekcje klasyfikacji A-H.
  Grupowanie ze względu na bardziej szczegółową klasyfikację
    jest problematyczne 
      ze względu na różnorodność 
        tych szczegółów.
  Istnieje również sekcja X, 
    która jest zarezerwowana dla
      przyszłych zastosowań technicznych --- 
    ona także nie jest analizowana,
      ponieważ występowanie patentów w tej sekcji 
        jest sporadyczne.

  \begin{table}[H]
    \begin{tabular}{|l|c|l|c|}
    \hline
    Dziedzina & Symbol & Dziedzina & Symbol \\
    \hline
    potrzeby ludzkie & A & górnictwo & \cellcolor{purple} E \\
    procesy przemysłowe & \cellcolor{yellow}B & budowa maszyn & \cellcolor{olive} F \\
    transport & \cellcolor{yellow}B & oświetlenie & \cellcolor{olive} F \\
    chemia & \cellcolor{teal}C & ogrzewanie & \cellcolor{olive} F \\
    metalurgia & \cellcolor{teal}C & uzbrojenie & \cellcolor{olive} F \\
    włókiennictwo & \cellcolor{pink} D & technika minerska & \cellcolor{olive} F \\
    papiernictwo & \cellcolor{pink} D & fizyka & \cellcolor{orange} G \\
    budownictwo & \cellcolor{purple} E & elektrotechnika & \cellcolor{brown} H \\
    \hline
    \end{tabular}
    \caption{Dziedziny klasyfikacji IPC}\footnotesize
    {Źródło: \url{https://www.wipo.int/web/classification-ipc}}
  \end{table}
  """

  H = storage
  a = assignpath

  with open(a, 'r') as f:
    H.assignement = yaml.load(f, Loader=yaml.FullLoader)

  K = ['IPC', 'IPCR', 'CPC', 'NPC']
  K0 = ['section', 'class']
  if extended: K0 = K0 + ['subclass', 'group', 'subgroup']

  U = [H.melt(k).reset_index() for k in K]
  U = [m for m in U if not m.empty]
  if not U: return pandas.DataFrame()
  C = pandas.concat(U)

  C['value'] = C['value'].str.replace(r'\s+', ' ', regex=True)
  C['section'] = C['value'].str.extract(r'^(\w)') 
  C['class'] = C['value'].str.extract(r'^\w\s?(\d+)')
  if extended:

    raise NotImplementedError("fixit")
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

  Y = pandas.concat([C, P], axis=0).drop(columns='id')
  Y.columns = ['doc', 'classification'] + K0
  Y = Y.set_index(['doc'])

  Y = Y.join(codes, how='left')

  assert { 'classification', 'section', 'class', 'country', 'number' }.issubset(Y.columns)
  assert { 'doc' }.issubset(Y.index.names)
  return Y

@lib.flow.placeholder()
def Geolocate(storage:lib.storage.Storage, 
             assignpath:str, 
             geodata:pandas.DataFrame, 
             codes:pandas.DataFrame,
             NAstr = ['bd', '~']):

  r"""
  \subsection{Dane lokalizacyjne}

  Urząd zbiera informacje dotyczące lokalizacji 
    osób 
      związanych z patentami. 
  Są to informacje 
    ograniczone 
      wyłącznie do nazwy miejscowości 
        jaką zadeklarowała dana osoba. 
  Jest to duże obciążenie dla analizy, 
    ponieważ nazwy miejscowości 
      nie są unikalne. 
  Nie ma także pewności co do 
    formatowania ich nazw. 
  Dla uproszczenia, 
    wszystkie nazwy miejscowości zostały 
      znormalizowane 
        poprzez zamianę znaków diaktrycznych
          na ich odpowiedniki bez diaktryków 
        oraz usunięcie znaków interpunkcyjnych.
  Dużym uproszczeniem jest 
    wykluczenie wszystkich nazw, 
      które nie dotyczą miejscowości o prawach miejskich ---
        wsie nie charakteryzyją się unikalnością 
          w zbyt dużej części, 
        dodatkowo nierzadko ich nazwy pokrywają się z nazwami miast.
      W Polsce współczynnik urbanizacji to niemal 60\%;
      miejscowości bez praw miejskich mają dużo niższe populacje ---
        można założyć, 
          że większość osób działających przy patentach
            melduje się w miastach.
  Problem zduplikowanych nazw miejscowości 
    (przykładowo Opole)
    został rozwiązany
      sprzez zastosowanie algorytmu wybierającego 
        najbliższe współrzędne geograficzne 
          dla danej grupy:
        rozważane są wszystkie kombinacje nazw miejscowości,
          kombinacja o najmniejszej sumie odległości jest wybierana.
  """

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  C = S.melt('city').drop(columns=['repo', 'col', 'frame', 'assignement'])
  C = C[ ~ C['value'].isin(NAstr) ]
  C = C.drop_duplicates(subset=['doc', 'value'])
  C = C.set_index('doc')
  C = C['value'].str.split(',').explode()
  C = C.str.split(';').explode()

 #Wyciąganie z zapisów kodów pocztowych
  C = C.str.extractall(r'((?:[^\d\W]|\s)+)')[0].rename('value').dropna()

  C = C.reset_index().drop(columns='match')
  C['value'] = C['value'].apply(strnorm, dropinter=True, dropdigit=True)
  C = C.set_index('value')

  L = geodata
  L = L[ L['type'] == 'miasto' ].copy()
  L['city'] = L['city'].apply(strnorm, dropinter=True, dropdigit=True)

  Y = C.join(L.set_index('city'), how='inner').reset_index()
  Y = Y.dropna(subset=['lat','lon']).dropna(axis=1)
  Y = Y[['doc', 'value', 'lat', 'lon']]
  Y = Y.rename(columns={'value': 'city'})
  Y = Y.drop_duplicates()

  Y['lat'] = pandas.to_numeric(Y['lat'])
  Y['lon'] = pandas.to_numeric(Y['lon'])
  Y = lib.geo.closest(Y, 'doc', 'city', 'lat', 'lon')

  Y = Y.set_index('doc')
  Y = Y.join(codes, how='left')

  assert { 'city', 'lat', 'lon', 'loceval', 'country', 'number' }.issubset(Y.columns)
  assert { 'doc' }.issubset(Y.index.names)
  return Y

repos = { k: dict() for k in D.keys() }

for h in repos.keys():

  repos[h]['code'] = Code(prfl.repos[h], assignpath=D[h]+'/assignement.yaml').map(D[h]+'/code/data.pkl')

  repos[h]['event'] = Event(prfl.repos[h], assignpath=D[h]+'/assignement.yaml',
                            codes=repos[h]['code']).map(D[h]+'/event/data.pkl')

  repos[h]['classify'] = Classify(prfl.repos[h], assignpath=D[h]+'/assignement.yaml',
                                  codes=repos[h]['code']).map(D[h]+'/classify/data.pkl')

  repos[h]['geoloc'] = Geolocate(prfl.repos[h], assignpath=D[h]+'/assignement.yaml', codes=repos[h]['code'], 
                                 geodata=gloc.geodata,).map(D[h]+'/geoloc/data.pkl')

for h in repos.keys():
  repos[h]['patentify'] = lib.flow.Flow(callback=lambda *x: x, args=[repos[h][k] for k in repos[h].keys()])

UPRP = repos['UPRP']
Lens = repos['Lens']
USPG = repos['USPG']
USPA = repos['USPA']
Google = repos['Google']

plots = dict()
for h in ['UPRP']:

  plots[f'F-{h}-event'] = lib.flow.Flow(args=[repos[h]['event']], callback=lambda X:

    X .assign(year=X['date'].dt.year.astype(int))\
      .value_counts(['event', 'year']).reset_index()\
      .query('year <= 1970')\
      .pipe(Plot.Chart).mark_bar()\
      .encode(Plot.Y('year:O').title('Rok rejestru'), 
              Plot.X('count').title(None),
              Plot.Color('event')\
                  .scale(scheme='category10')\
                  .title('Rodzaj rejestru')) | \
    X .assign(year=X['date'].dt.year.astype(int))\
      .value_counts(['event', 'year']).reset_index()\
      .query('year > 1970')\
      .pipe(Plot.Chart).mark_bar()\
      .encode(Plot.Y('year:O').title(None), 
              Plot.X('count').title(None),
              Plot.Color('event')\
                  .scale(scheme='category10')\
                  .title('Rodzaj rejestru')\
                  .legend(orient='bottom') )
)

for k, F in plots.items():
  F.name = k
  F.map(f'fig/patt/{k}.png')