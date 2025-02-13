r"""
\section{Dane patentowe}

\subsection{Role patentowe w aplikacjach patentowych}

\begin{figure}[H]
\centering
\begin{tikzpicture}
	\draw[draw=black, fill=lightgray, thin, solid] (-2.00,2.00) rectangle (-1.00,0.50);
	\node[black, anchor=south west] at (-3.06,2.25) {patent};
	\draw[draw=black, thin, solid] (-1.00,1.50) -- (1.00,4.00);
	\draw[draw=black, thin, solid] (-1.00,1.00) -- (1.00,-2.00);
	\node[black, anchor=south west] at (1.94,-2.25) {wynalazek};
	\node[black, anchor=south west] at (1.94,-0.25) {wynalazca};
	\draw[draw=black, thin, solid] (-1.00,1.50) -- (1.00,2.00);
	\draw[draw=black, thin, solid] (-1.00,1.50) -- (1.00,0.00);
	\node[black, anchor=south west] at (1.94,1.75) {aplikant};
	\node[black, anchor=south west] at (1.94,3.75) {właściciel};
	\draw[draw=black, thin, solid] (1.50,4.00) ellipse (0.50 and 0.50);
	\draw[draw=black, thin, solid] (1.50,2.00) ellipse (0.50 and 0.50);
	\draw[draw=black, thin, solid] (1.50,0.00) ellipse (0.50 and 0.50);
	\draw[draw=black, fill=black, thin, solid] (-1.00,1.50) circle (0.1);
	\draw[draw=black, fill=black, thin, solid] (-1.00,1.00) circle (0.1);
	\draw[draw=black, fill=black, thin, solid] (1.00,-1.50) rectangle (2.00,-2.50);
	\node[black, anchor=south west] at (-5.06,3.25) {biuro};
	\draw[draw=black, thin, solid] (-1.50,4.00) ellipse (0.50 and -0.50);
	\draw[draw=black, thin, solid] (-5.00,3.00) rectangle (-4.00,2.00);
	\node[black, anchor=south west] at (-1.2,4.5) {pełnomocnik};
	\draw[draw=black, thin, solid] (-1.50,2.00) -- (-1.50,3.50);
	\draw[draw=black, fill=black, thin, solid] (-1.50,2.00) circle (0.1);
	\draw[draw=black, fill=black, thin, solid] (-2.00,1.50) circle (0.1);
	\draw[draw=black, thin, solid] (-2.00,1.50) -- (-4.00,2.50);
	\draw[draw=black, thin, solid] (-4.50,0.00) ellipse (0.50 and -0.50);
	\node[black, anchor=south west] at (-5.06,0.75) {urzędnik};
	\node[black, anchor=south west] at (-3.06,-3.25) {raport};
	\draw[draw=black, fill=gray, thin, solid] (-4.00,-2.00) rectangle (-3.00,-3.50);
	\draw[draw=black, thin, solid] ([shift=(90:0.50 and -1.25)]-5.00,1.25) arc (90:270:0.50 and -1.25);
	\draw[draw=black, thin, solid] (-4.50,-0.50) -- (-4.00,-2.00);
	\draw[draw=black, fill=black, thin, solid] (-4.00,-2.00) circle (0.1);
	\draw[draw=black, thin, dotted] (-4.00,0.00) -- (-2.00,1.50);
	\draw[draw=black, thin, solid] (-3.00,-2.00) -- (-2.00,1.00);
	\draw[draw=black, fill=black, thin, solid] (-2.00,1.00) circle (0.1);
	\draw[draw=black, fill=black, thin, solid] (-3.00,-2.00) circle (0.1);
\end{tikzpicture}
\caption{Struktura powiązań patentu}
\label{fig:struktura-patentowa}
\end{figure}

\begin{defi}
Wynalazca --- osoba podająca się za autora bądź współautora nowej
wiedzy technicznej.
\end{defi}

\begin{defi}
Wynalazek --- nowa wiedza techniczna, która jest opatentowana.
\end{defi}

\begin{defi}
\label{defi:wynalazca}
Wynalazca --- osoba podająca się za autora bądź współautora nowej
wiedzy technicznej.
\end{defi}

\begin{defi}
\label{defi:aplikant}
Aplikant --- osoba składająca wniosek patentowy na podstawie autorstwa,
albo innych przesłanek do własności nad patentem; przykładowo patent
może być efektem pracy w organizacji w zatrudnieniu --- wtedy to
organizacja może być składać wniosek patentowy.
\end{defi}

\begin{defi}
Właściciel --- osoba posiadająca prawo do patentu; może je utrzymać
na przykład w wyniku sprzedaży.
\end{defi}

\begin{defi}
Pełnomocnik --- osoba wykonująca czynności urzędowe związane z
utrzymaniem patentu w mocy; może to być wyznaczona osoba niepowiązana z 
patentem, ale posiadająca uprawenienia wymagane przez urząd, albo
osoba fizyczna współuprawniona bądź z bliskiej rodziny.
\end{defi}

\begin{defi}
Biuro --- instytucja zajmująca się przyznawaniem patentów.
\end{defi}

\begin{defi}
Urzędnik --- tutaj: pracownik biura wykonujący raport o stanie
techniki dla danego patentu.
\end{defi}


\newpage\begin{acronym}

\acro
{UPRP}{Urząd Patentowy Rzeczypospolitej Polskiej}

\acro
{EPO}{European Patent Office}

\acro
{WIPO}{World Intellectual Property Organization}

\acro
{MKP}{Międzynarodowa Klasyfikacja Patentów}

\acro
{IPC}{International Patent Classification}

\acro
{API}{Application Programming Interface}

\acro
{URI}{Uniform Resource Identifier}

\acro
{URL}{Uniform Resource Locator}

\acro
{OCR}{Optical Character Recognition}

\acro
{XML}{Extensible Markup Language}

\acro
{PDF}{Portable Document Format}

\end{acronym}
"""

#lib
import lib.flow, lib.geo, lib.storage, gloc
from util import strnorm, data as D
from prfl import flow as f0

#calc
import pandas, yaml

#plot
import altair as Plot

@lib.flow.make()
def code(storage:lib.storage.Storage, assignpath:str):

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

@lib.flow.make()
def event(storage:lib.storage.Storage, assignpath:str, codes:pandas.DataFrame):

  r"""
  \subsection
  {Rejestr dat związanych z patentami}

  Poszczególne czynności związane z ochroną patentów są rejestrowane.
  Każde wydarzenie jest powiązane z konkretną datą kalendarzową.
  Można wyróżnić kilka typów wydarzeń związanych z patentami:

    \begin{itemize}

  \item
  publiczne ujawnienie \foreign{ang}{exhibition};


  \item
  roszczenia z pierwszeństwa \foreign{ang}{priority claim} --- 
  data rozszczenia sprzed rozpoczęcia procesu patentowania dla wybranego urzędu;


  \item
  regionalna deklaracja \foreign{ang}{regional filing};


  \item
  deklaracja \foreign{ang}{filing};


  \item
  aplikacja \foreign{ang}{application} --- data złożenia aplikacji;


  \item
  przyznanie ochrony \foreign{ang}{grant};


  \item
  decyzja urzędowa;


  \item
  publikacja.
  \end{itemize}



  \newpage
  \figpage{0.8}{../fig/patt/F-UPRP-event.png}
  {Wydarzenia związane z patentami w kolejnych latach}

  Wykres obrazuje kolejne lata i to jakie
  działania podejmował urząd w stosunku do składanych patentów.



  \newpage
  \figsides
  {../fig/rgst/F-grant-delay.png}
  {Okres po między złożeniem aplikacji, a przyznaniem ochrony w Polsce}
  {../fig/rgst/F-grant-delay.png}
  { Okres po między złożeniem aplikacji, a przyznaniem ochrony w Polsce 
    w latach 2013-2022 }

  \figside
  {../fig/rgst/F-application-grant.png}
  { Lata składania aplikacji dla patentów, które otrzymały ochrone w Polsce
    w latach 2013-2022 }
  { W analizie czasowej opartej o patenty istotny jest fakt, że przyznawanie ochrony
    nie jest natychmiastowe. W Polsce średni czas oczekiwania na ochronę wynosi
    5.5 roku, chociaż najczęściej nie przekracza on okresu 5 lat (mediana).
    W związku z tym wszelkie wnioski dotyczące innowacji w Polsce są istotnie
    opóźnione w stosunku do powstawania wynalazków, przeciętnie o niespełna 5 lat. }


  \newpage
  \figside
  {../fig/subj/F-geoloc-eval.png}
  { Stan uzupełnienia informacji o geolokalizacjach, w Polsce, 
    osób i organizacji pełniących role patentowe
    w aplikacjach patentowych w zależności od roku}

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

@lib.flow.make()
def classify(storage:lib.storage.Storage, assignpath:str, codes:pandas.DataFrame, extended=False):

  """
  \newpage\subsection
  {Klasyfikacje patentów}

  Klasyfikacje patentowe to systemy, które pozwalają na przypisanie
  patentów do odpowiednich dziedzin.



  \subsubsection
  {Międzynarodowa Klasyfikacja Patentów}
  \label{IPC}

  W Polsce funkcjonuje klasyfikacja
  \ac{MKP}, czyli \ac{IPC}. Zapis klasyfikacji w tym systemie to ciąg
  cyfrowo-literowy składający się z 4 części:



  \begin{enumerate}

  \item Dział - najwyższa hierarchia złożona z 8 kategorii

  \begin{itemize}
  \item ma tytuł informacyjny
  \item każdy tytuł działu ma swój symbol: A, B, C, D, E, F, G albo H

  \begin{itemize}
  \item A – podstawowe potrzeby ludzkie
  \item B – różne procesy przemysłowe; transport
  \item C – chemia; metalurgia
  \item D – włókiennictwo; papiernictwo
  \item E – budownictwo; górnictwo
  \item F – budowa maszyn; oświetlenie; ogrzewanie; uzbrojenie; technika minerska
  \item G – fizyka
  \item H – elektrotechnika
  \end{itemize}

  \item poddział - każdy dział może zawierać poddział, który nie jest oznaczany symbolem
  \end{itemize}



  \item Klasa - drugi poziom hierarchii

  \begin{itemize}
  \item ma tytuł informacyjny
  \item oznaczana przez liczbę 2-cyfrową
  \item zakres klasy - skrótowa informacja o treści klasy
  \end{itemize}



  \item Podklasa - trzeci poziom hierarchii

  \begin{itemize}
  \item ma tytuł informacyjny
  \item oznaczana dużą literą
  \item ma zakres i tytuł pomocniczy
  \end{itemize}



  \item Grupa - czwarty poziom hierarchii

  \begin{itemize}
  \item 2 zestawy cyfr oddzielone ukośnikiem

  \begin{itemize}
  \item zestaw pierwszy składa się od 1 do 3 cyfr i określa grupę główną
  \item zestaw drugi składa się z 2 cyfr i określa grupę pomocniczą, grupa główna jest oznaczana 00
  \end{itemize}

  \item grupa ma tytuł informacyjny, podgrupa ma bardziej szczegółowe hasło
  \end{itemize}

  \end{enumerate}




  \figside
  {../fig/subj/F-geoloc-eval-clsf.png}
  { Stan uzupełnienia informacji o geolokalizacjach, w Polsce, 
    osób i organizacji pełniących role patentowe
    w aplikacjach patentowych w zależności od klasyfikacji patentu}
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

@lib.flow.make()
def geolocate(storage:lib.storage.Storage, 
             assignpath:str, 
             geodata:pandas.DataFrame, 
             codes:pandas.DataFrame,
             NAstr = ['bd', '~']):

  r"""
  \subsection{Dane przestrzenne}

  Dane przestrzenne odnoszą się do miejsc z jakimi są powiązane
  osoby albo organizacje związane z patentami. Pozostawia to więc 
  różne możliwości analizy przestrzennej:

  \begin{enumerate}
  \item[$A$:] przypisanie każdego patentu do pojedynczej lokalizacji;
  \item[$B$:] przypisanie patentu do wielu lokalizacji.
  \end{enumerate}

  W przypadku $A$ powstaje problem przypisania głównej lokalizacji.
  Jest to kwestia $A_1$ priorytetowania organizacji ponad osoby, bądź
  odwrotnie, oraz $A_2$ wyboru głównej osoby/organizacji.
  Problem $A_1$ wiąże się z potencjalnymi różnicami w modelu zależnie
  od wybranego podejścia. Problem $A_2$ może być niejednoznaczny
  w rozwiązaniu z powodu zbyt małego zakresu informacji zawartych w danych.
  Wymagałoby to dodatkowych danych z samego procesu powstawania wynalazku,
  co jest poza zakresem tej pracy.

  Dalsza analiza odnosi się wyłącznie do podejścia $B$.
  Patent może mieć więc kilka lokalizacji, żadna nie jest określona jako główna.

  \newpage
  Opis tego jak uzupełnione są braki danych znajduje się w kolejnej sekcji.
  Obok znajduje się wykres ilustrujący ilość patentów z geolokalizacjiami,
  zgodnie z tym jak zostały określone.

  \fig{../fig/rgst/F-geoloc-eval-appl.png}
  { Stan uzupełnienia informacji o geolokalizacjach, w Polsce, 
    osób i organizacji  pełniących role patentowe
    w aplikacjach patentowych}

  \fig{../fig/rgst/F-geoloc-eval-grant.png}
  { Stan uzupełnienia informacji o geolokalizacjach, w Polsce, 
    osób i organizacji  pełniących role patentowe
    w aplikacjach patentowych, które otrzymały ochronę}
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

flow = { k: dict() for k in D.keys() }

for h in flow.keys():

  flow[h]['code'] = code(f0[h]['profiling'], assignpath=D[h]+'/assignement.yaml').map(D[h]+'/code/data.pkl')

  flow[h]['event'] = event(f0[h]['profiling'], 
                           assignpath=D[h]+'/assignement.yaml',
                           codes=flow[h]['code']).map(D[h]+'/event/data.pkl')

  flow[h]['classify'] = classify(f0[h]['profiling'],
                                 assignpath=D[h]+'/assignement.yaml',
                                 codes=flow[h]['code']).map(D[h]+'/classify/data.pkl')

  flow[h]['geoloc'] = geolocate(f0[h]['profiling'], 
                                assignpath=D[h]+'/assignement.yaml', codes=flow[h]['code'], 
                                geodata=gloc.geodata,).map(D[h]+'/geoloc/data.pkl')

for h in flow.keys():
  flow[h]['patentify'] = lib.flow.Flow(callback=lambda *x: x, args=[flow[h][k] for k in flow[h].keys()])


plots = dict()
for h in ['UPRP']:

  plots[f'F-{h}-event'] = lib.flow.Flow(args=[flow[h]['event']], callback=lambda X:

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