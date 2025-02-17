r"""
\section{Dane dotyczące osób i organizacji}

Oprócz zbieranie informacji o samych patentach, 
urząd zbiera także informacje o osobach związanych z patentami
i organizacjach.
Wyżej wspomniane są nazwy miejscowości zameldowania,
oprócz nich urząd zbiera także imię i nazwisko
albo nazwę organizacji.
Pozyskiwanie tych informacji dotyczy 
osób zaangażowanych w proces patentowy 
z różnych pobudek:
prawnych, organizacyjnych, finansowych i innych.

  \begin{multicols}{2}

Można wyróżnić 5 ról w procesie patentowym:
wynalazca, właściciel, aplikant, pełnomocnik oraz urzędnik.
Wynalazca jest (współ)autorem wynalazku, właściciel posiada
prawa do patentu, aplikant składa wniosek patentowy, pełnomocnik
reprezentuje właściciela w sprawach urzędowych, 
a wskazany na rysunku \ref{struktura-patentowa} obok urzędnik
przygotowuje raport o stanie techniki dla danego patentu.

\columnbreak

\begin{figure}[H]
\centering\footnotesize
\begin{tikzpicture}[scale=0.5]
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
	\node[black, anchor=south west] at (-2.2,4.5) {pełnomocnik};
	\draw[draw=black, thin, solid] (-1.50,2.00) -- (-1.50,3.50);
	\draw[draw=black, fill=black, thin, solid] (-1.50,2.00) circle (0.1);
	\draw[draw=black, fill=black, thin, solid] (-2.00,1.50) circle (0.1);
	\draw[draw=black, thin, solid] (-2.00,1.50) -- (-4.00,2.50);
	\draw[draw=black, thin, solid] (-4.50,0.00) ellipse (0.50 and -0.50);
	\node[black, anchor=south west] at (-6.06,0.55) {urzędnik};
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
\label{struktura-patentowa}
\end{figure}
\end{multicols}

Głownym problemen danych jest 
niejednoznaczność w kontekście identyfikacji osób i organizacji.
W danych patentowych,
osoby rozróżnia się za pomocą imienia, nazwiska oraz nazwy miejscowości. 
Jak wiadomo wiele osób może mieć te same imię i nazwisko, także w jednym miejscu.
Należy także wspomnieć o drobnych niespójnościach danych w zapisie imion i nazwisk
--- występowanie diaktryk i akcentów w zapisie
nie jest gwarantowane, 
a jednocześnie nie jest wykluczone;
niektóre podpisy zawierają pierwsze litery imion, inne są całościowe 
mimo że dotyczą potencjalnie tej samej osoby.
Nie ma także pełnego podziału na osoby i organizacje. 
W wielu przypadkach organizacja rzeczywiście 
jest oznaczona jako taki podmiot,
jednak nie jest to regułą.
"""

#lib
import lib.storage, lib.name, lib.flow, patt, prfl
from util import strnorm, data as D

#calc
import pandas, yaml, numpy

#plot
import altair as Pt
from util import A4

@lib.flow.map('cache/names.pkl')
@lib.flow.init({D['UPRP']+'/assignement.yaml':   prfl.repos['UPRP'],
                D['Lens']+'/assignement.yaml':   prfl.repos['Lens'],
                D['Google']+'/assignement.yaml': prfl.repos['Google'] })
def names(asnstores:dict[lib.storage.Storage, str],
          assignements = ['names', 'firstnames', 'lastnames', 'ambignames'],
          typeassign='type-name'):

  r"""
  \subsection{Oznaczanie nazw}

  Wykorzystując fakt, że niektóre nazwy są oznaczone jako organizacje,
  a inne zawierają podział na imię i nazwisko można ustalić zbiór słów
  kluczowych oraz pełnych nazw, które są charakterystyczne dla danego typu.
  Dodatkową informacją jest to, że
  o ile aplikanci i właściciele patentów mogą dotyczyć zarówno osób
  fizycznych jak i organizacji, to wynalazcy są zawsze osobami fizycznymi.
  Do zbioru wprowadzone są także słowa charakterystyczne dla nazw organizacji,
  takie jak \textit{spółka}, \textit{fundacja}, \textit{instytut}, 
  \textit{INC.}, \textit{SP. Z O. O.} itp. Zbiór ten wykorzystany jest dalej
  do oceny czy dany wpis dotyczy osoby fizycznej czy organizacji.
  """

  Y = pandas.DataFrame()

  for assignpath, S in asnstores.items():

    with open(assignpath, 'r') as f:
      S.assignement = yaml.load(f, Loader=yaml.FullLoader)

    for h in ['assignee', 'applicant', 'inventor']:

      K = [f'{k0}-{h}' for k0 in assignements]

      X = pandas.concat([S.melt(f'{k}') for k in K]).set_index(['doc', 'id'])
      X = X[['value', 'assignement']]
      X['assignement'] = X['assignement'].str.split('-').str[0]

      T = S.melt(typeassign).set_index(['doc', 'id'])['value'].rename('type')
      if not T.empty: X = X.join(T, on=['doc', 'id'], how='left')

      Y = pandas.concat([Y, X]) if not Y.empty else X

  return lib.name.mapnames(Y.reset_index(drop=True), 
                  orgqueries=['type.str.upper() == "LEGAL"'],
                  orgkeysubstr=['&', 'INTERNAZIO', 'INTERNATIO', 'INC.', 'ING.', 'SP. Z O. O.', 'S.P.A.'],
                  orgkeywords=[x for X in [ 'THE', 'INDIVIDUAL', 'CORP',
                                            'COMPANY PRZEDSIEBIORSTWO FUNDACJA INSTYTUT INSTITUTE',
                                            'HOSPITAL SZPITAL',
                                            'SZKOLA',
                                            'COMPANY LTD SPOLKA LIMITED GMBH ZAKLAD PPHU',
                                            'KOPALNIA SPOLDZIELNIA FABRYKA',
                                            'ENTERPRISE TECHNOLOGY',
                                            'LLC CORPORATION INC',
                                            'MIASTO GMINA URZAD',
                                            'GOVERNMENT RZAD',
                                            'AKTIENGESELLSCHAFT KOMMANDITGESELLSCHAFT',
                                            'UNIWERSYTET UNIVERSITY AKADEMIA ACADEMY',
                                            'POLITECHNIKA'] for x in X.split()])

@lib.flow.map('cache/pulled.pkl')
@lib.flow.init(prfl.UPRP, assignpath=D['UPRP']+'/assignement.yaml')
def pulled( storage:lib.storage.Storage, assignpath:str,
          assignements = ['firstnames', 'lastnames'],
          assignentities = ['names', 'ambignames'],
          cityassign='city'):

  r"""
  \subsection{Wyciąganie danych o osobach i organizacjach}

  Dane dotyczące osób wyciągane są ze zbioru danych z pewnymi zastrzeżeniami:

  \begin{itemize}
  \item każda informacja o osobie/organizacji istnieje w ramach jakiegoś patentu;
  \item jedna osoba/organizacja może pełnić wiele ról patentowych;
  \item nazwy organizacji nie są w pełni znormalizowane;
  \item imiona i nazwiska są znormalizowane;
  \item nazwy miejscości są znormalizowane;
  \item osoby o identycznych imionach i nazwiskach są traktowane jako różne
        jeśli inna jest ich miejscowość;
  \end{itemize}

  Normalizacja w tym przypadku to usuwanie znaków interpunkcyjnych
  oraz zastąpienie znaków diaktrycznych ich odpowiednikami bez diaktryków.
  """

  S = storage
  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  Y = pandas.DataFrame()

  C = S.melt(cityassign).set_index(['doc','id'])['value'].rename('city')

  i = 0
  for h in ['assignee', 'applicant', 'inventor']:

    X0 = pandas.concat([S.melt(f'{k}') for k in [f'{k0}-{h}' for k0 in assignements]])
    if not X0.empty:
      X0 = X0.set_index(['doc', 'id']).join(C, how='left').reset_index()
      X0['id'] = X0.groupby(['doc', 'id']).ngroup() + i
      i = X0['id'].max() + 1

    X = pandas.concat([S.melt(f'{k}') for k in [f'{k0}-{h}' for k0 in assignentities]])
    if not X.empty:
      X = X.set_index(['doc', 'id']).join(C, how='left').reset_index()
      X['id'] = X.groupby(['doc', 'id', 'frame', 'col']).ngroup() + i
      i = X['id'].max() + 1

    if 'city' not in X.columns: X['city'] = None
    if 'city' not in X0.columns: X0['city'] = None

    X = pandas.concat([X0[['doc', 'id', 'value', 'city']], 
                       X[['doc', 'id', 'value', 'city']]])

    X = X.set_index(['doc', 'id'])
    X[h] = True

    Y = pandas.concat([Y, X]) if not Y.empty else X

 #Roles
  Y[['assignee', 'applicant', 'inventor']] = Y[['assignee', 'applicant', 'inventor']].fillna(False)
  Y = Y.reset_index().groupby(['doc', 'id', 'city'], dropna=False)\
       .agg({'assignee': 'max', 'applicant': 'max', 'inventor': 'max', 
             'value': ' '.join }).reset_index()

 #Normalize
  Y['city'] = Y['city'].apply(strnorm, dropinter=True, dropdigit=True)
  Y['value'] = Y['value'].apply(strnorm, dropinter=False, dropdigit=False)

 #Traktowanie powtórzeń wynikających z nazw miast jako inne rejestry
  Y = Y.drop_duplicates(['doc', 'value', 'city'])

 #Concat'val
  Y['id'] = numpy.arange(Y.shape[0])
  Y = Y.set_index('id').drop_duplicates()
  assert { 'id' }.issubset(Y.index.names) and Y.index.is_unique
  assert { 'doc', 'value', 'city', 
           'assignee', 'applicant', 'inventor' }.issubset(Y.columns)

  return Y

@lib.flow.map('cache/textual.pkl')
@lib.flow.init(pulled, nameset=names)
def named(pulled:pandas.DataFrame, nameset:pandas.DataFrame):

  r"""
  \subsection{Wyciąganie danych osobowych}

  Na podstawie wcześniej określonych nazw charakterystycznych
  dla organizacji i tych dla imion, dane dotyczące podmiotów
  są dzielone na dane osobowe i dotyczące organizacji.
  Część nazw i imion nie ulega klasyfikacji w wyniku algorytmu. 
  Dla uproszczenia zakładamy, że dotyczą one osób fizycznych.

  Definiujemy przy tym ciąg imienniczy, który wykorzystany jest
  później przy identyfikacji tych samych osób jako autorów
  różnych patentów.

  \D{ciąg-imienniczy}{Ciąg imienniczy $N_k$}
  { ciąg imion oraz nazwisk przypisany danej osobie. }

  To czy dany element $n,\ n\in N_k$ jest imieniem, czy nazwiskiem 
  nie zawsze jest jednoznaczne.
  """

  X = pulled
  M = nameset

  assert { 'id' }.issubset(X.index.names) and X.index.is_unique
  assert X.duplicated(['doc', 'value', 'city']).sum() == 0
  assert { 'doc', 'value', 'city', 
           'assignee', 'applicant', 'inventor' }.issubset(X.columns)

 #exact
  X = X.reset_index()
  E = X.set_index('value').join(M, how='inner')
  E = E.reset_index().set_index(['doc', 'id', 'city'])
  X = X.set_index(['doc', 'id', 'city']).drop(E.index)

 #split
  X['value'] = X['value'].apply(strnorm, dropinter=True, dropdigit=True)
  X['nword'] = X['value'].str.count(' ') + 1
  X['value'] = X['value'].str.split(' ')

 #word
  W = X.reset_index().explode('value')
  W = W.set_index('value').join(M[ M.isin(['firstname', 'lastname', 'ambigname']) ], how='inner')
  nW = W.groupby(['doc', 'id', 'city']).agg({'nword': 'first', 'role': 'count'})
  W = nW[ nW['nword'] == nW['role'] ][[]].join(W.reset_index().set_index(['doc', 'id', 'city']), how='inner')
  X = X.drop('nword', axis=1).drop(W.index)
  X['value'] = X['value'].apply(' '.join)
  W = W.drop('nword', axis=1)

  Y = pandas.concat([E, W]).reset_index().drop_duplicates(['doc', 'city', 'value'])

 #org
  O = Y[ Y['role'] == 'orgname' ]
  O = O.set_index('id').drop('role', axis=1)
  O['organisation'] = True

  assert { 'id' }.issubset(O.index.names)
  assert { 'organisation'  }.issubset(O.columns)

 #people
  agg = lambda X: X.agg({'value':' '.join, **{k:'max' for k in ['assignee', 'inventor', 'applicant'] }})
  P  = Y[ Y['role'] !=   'orgname' ].groupby(['doc', 'id', 'city']).pipe(agg)
  Nf = Y[ Y['role'] == 'firstname' ].groupby(['doc', 'id', 'city']).pipe(agg)['value'].rename('firstnames')
  Nl = Y[ Y['role'] ==  'lastname' ].groupby(['doc', 'id', 'city']).pipe(agg)['value'].rename('lastnames')
  P = P.join(Nf, how='left').join(Nl, how='left').reset_index().set_index('id')
  P['organisation'] = False

  assert { 'id' }.issubset(P.index.names)
  assert { 'organisation', 'firstnames', 'lastnames' }.issubset(P.columns)

  X = X.reset_index().set_index('id')
  X = X.loc[X.index.difference(P.index).difference(O.index)]
  Y = pandas.concat([P, O, X])

  assert { 'id' }.issubset(Y.index.names) and Y.index.is_unique
  assert { 'doc', 'value', 'firstnames', 'lastnames', 'city', 
           'organisation', 'assignee', 'applicant', 'inventor' }.issubset(Y.columns)

  return Y

@lib.flow.map('cache/spacetime.pkl')
@lib.flow.init(named, patt.UPRP['geoloc'], patt.UPRP['event'], patt.UPRP['classify'])
def placed(textual:pandas.DataFrame, geoloc:pandas.DataFrame, event:pandas.DataFrame, clsf:pandas.DataFrame):

  r"""
  \subsection{Umieszczenie osób w czasie i przestrzeni}

  Biorąc pod uwagę dane pozyskane na temat patentów oraz 
  osób z nimi związanych można zdefiniować przestrzeń
  oraz czas w jakim te osoby działają.
  Każdej osobie przyporządkowany jest odpowiedni punkt, 
  wcześniej określony przy ustalaniu lokalizacji patentów.
  Czas działania osoby jest wyznaczony jako czas złożenia
  aplikacji patentowej --- to w trakcie składania aplikacji
  osoby podają swoje dane w urzędzie.
  Osobę charakteryzują również klasyfikacje \ac{IPC},
  do których przypisany był patent.
  Każda sekcja klasyfikacji, do której przypisany był patent
  jest zliczana w ramach danej osoby.

  \chart{fig/rgst/F-loceval.pdf}
  { Wykres uzupełnienia danych na temat położenia osób pełniących role patentowe }
  Powyższe wykresy prezentują stan uzupełnienia danych na temat położenia osób
  pełniących role patentowe w czasie. Pierwszy wykres od góry prezentuje
  liczności osób, które składały aplikacje patentowe w danym roku.
  Drugi ogranicza się wyłącznie do osób, które składały patenty uzyskujące ochronę.
  Na rysnuku widać, że w dużej części przypadków nie udało się ustalić położenia
  danej osoby, szczególnie przed rokiem 2004. 

  W badanym okresie 2013-2022 także
  widać istotną frakcję osób, 
  co do których nie udało się ustalić położenia.
  Jest to około 28\% wszystkich patentów,
  biorąc pod uwagę tylko patenty z ochroną: 26\%.
  Należy zaznaczyć ich równomierne rozłożenie w czasie,
  co sugeruje, że nie jest to związane z określonymi
  okresami, a raczej z błędem systematycznym, 
  jakim może być wcześniej wspomniane wykluczenie
  pomniejszych miejscowości.
  """

  X = textual

  T = event
  X = X.reset_index().set_index('doc')\
      .join(T.groupby('doc')['date'].min(), how='left').reset_index()
  X = X.rename(columns={'date': 'firstdate'})

  for t in ['application', 'grant', 'nogrant', 'publication', 'decision', 'regional', 'exhibition']:

    A = T[ T['event'] == t ].reset_index().drop_duplicates(['doc', 'date'])
    X = X.set_index('doc')\
        .join(A.set_index('doc')['date'], how='left').reset_index()
    X = X.rename(columns={'date': t})

  G = geoloc.set_index('city', append=True)
  X = X.set_index(['doc', 'city']).join(G, how='left').reset_index()

  X = X.set_index('doc')
  C0 = clsf
  C0['clsf'] = C0['section']
  C0 = pandas.get_dummies(C0[['clsf']], prefix_sep='-')
  C0 = C0.groupby('doc').sum()
  X = X.join(C0, how='left')

  for c in ['IPC', 'IPCR', 'CPC']:

    C = clsf[ clsf['classification'] == c ]
    C = C.reset_index().drop_duplicates(['doc', 'classification', 'section'])
    C = C.pivot_table(index='doc', columns='classification', 
                      values='section', aggfunc=list)
    X = X.join(C, how='left')

  X = X.reset_index().set_index('id')

  assert { 'doc', 'lat', 'lon', 'loceval', 'firstdate', 'application', 'organisation' }.issubset(X.columns)
  assert any([ c.startswith('clsf-') for c in X.columns ])
  assert { 'id' }.issubset(X.index.names)

  return X

@lib.flow.map('fig/rgst/F-loceval.pdf')
@lib.flow.init(placed)
def evalplot(X:pandas.DataFrame):

  X = X.copy()
  X['year'] = X['application'].dt.year
  X['loceval'] = X['loceval'].fillna(~X['city'].isna())
  X['loceval'] = X['loceval'].replace({ True: 'nie znaleziono',
                                        False: 'nie podano',
                                        'unique': 'jednoznaczna',
                                        'proximity': 'najlbiższa innym' })

  N = X.value_counts(['year', 'loceval']).reset_index()
  F = Pt.Chart(N).mark_bar()
  F = F.encode( Pt.X('year:O').title(None)\
                  .axis(labelAngle=0, values=[1984, 1989, 2000, 2004, 2013, 2023]))
  F = F.encode( Pt.Y('count:Q').title('Z aplikacjami')\
                  .axis(values=[1000, 10000, 20000, 40000]))
  F = F.encode( Pt.Color('loceval:N')\
                  .title('Geolokalizacja'))

  Xg = X.dropna(subset=['grant'])
  Ng = Xg.value_counts(['year', 'loceval']).reset_index()
  Fg = Pt.Chart(Ng).mark_bar()
  Fg = Fg.encode( Pt.X('year:O').title('Rok')\
                  .axis(labelAngle=0, values=[1984, 1989, 2000, 2004, 2013, 2023]))
  Fg = Fg.encode( Pt.Y('count:Q').title('Z grantem')\
                  .axis(values=[1000, 10000, 20000, 40000]))
  Fg = Fg.encode( Pt.Color('loceval:N')\
                  .title('Geolokalizacja'))

  F = F.properties(width=0.8*A4.W, height=0.1*A4.H)
  Fg = Fg.properties(width=0.8*A4.W, height=0.1*A4.H)

  return F & Fg

@lib.flow.init(placed)
def selected(X:pandas.DataFrame):

  r"""
  \subsubsection{Kryterium czasowe wyboru danych}
  """

  A = (X['application'] >= '2013-01-01') & (X['application'] <= '2022-12-31') 
  G = (X['grant'] >= '2013-01-01') & (X['grant'] <= '2022-12-31')

  return X[A & G]