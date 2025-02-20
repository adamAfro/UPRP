r"""
\section{Dane z raportów o stanie techniki}
\label{rprt}

Na stronie \ac{UPRP} wyróżniono etapy w procesie patentowania.
Etapem po spełnieniu formalności jest 
\textit{sprawozdanie ze stanu techniki}\cite{UPRP-pat-prd}.
Słownik urzędu wskazuje, że \textit{stan techniki} to wszelka
wiedza dostępna do wskazanej daty powszechnie, albo taka,
która nie jest publiczna, ale została już ogłoszona 
w określony sposób\cite{UPRP-dict}.

Sprawozdanie jest realizowane przez urzędników i składa się z
klasyfikacji zgłoszenia, informacji o innych klasyfikacjach,
w których prowadzono poszukiwania, wykaz baz komputerowych,
użytych w trakcie procesu oraz tabelę zawierającą odniesienia
do innych prac. Wśród odniesień można wyróżnić odniesienia do
patentów, artykułów naukowych, książek, stron internetowych,
a także do innych zgłoszeń patentowych.

\begin{figure}[H]\centering
\label{fig:raport-biblio-ex}
\includegraphics[width=0.8\textwidth]{fig/img/rprt/bilblio-ex-1.jpg}
\caption{Przykład odniesienia do literatury technicznej 
         w raporcie o stanie techniki.}
\end{figure}
\begin{figure}[H]\centering
\label{fig:raport-pat-ex}
\includegraphics[width=0.8\textwidth]{fig/img/rprt/pat-ex-1.jpg}
\caption{Przykład odniesienia do innych patentów 
         w raporcie o stanie techniki.}
\end{figure}
\begin{figure}[H]\centering
\label{fig:raport-url-ex}
\includegraphics[width=0.8\textwidth]{fig/img/rprt/url-ex-1.jpg}
\caption{Przykład odniesienia do strony internetowej 
         w raporcie o stanie techniki.}
\end{figure}
\newpage

Z danych zebranych z \ac{API} można wyodrębnić tabelę 
zawierającą listę adresów internetowych
z plikami związanymi z danym zgłoszeniem. Tabela składa się
z kodów \ac{URI} oraz kodów rozróżniających typ dokumentu.
Kody o typie \textit{RAPORT} albo \textit{RAPORT1} są kodami 
\ac{URI} do z adresami \ac{URL} do plików zawierających raporty 
o stanie techniki. Są to pliki w formacie \ac{PDF}.

Poniżej przedstawiono przykład tego jaką strukturę mogą tworzyć 
\cref{fig:raport-ex}. Są to raporty dla patentów $p_1, p_2, p_3$. 
Zawierają odniesienia do patentów, które istnieją w zbiorze danych, 
tj. $p_1, p_2, p_3$. Oprócz tego mają odniesienia do patentów 
spoza domeny $\hat p_4, \hat p_5$ oraz publikacji naukowych 
$\hat l_1, \hat l_2$, które nie są uwzględnione w poniższej analizie.

\begin{figure}[H]\centering
\begin{tikzpicture}
	\draw[draw=black, thin, solid] (-5.00,3.00) rectangle (-3.00,0.00);
	\draw[draw=black, thin, solid] (-1.50,3.50) rectangle (1.50,-0.50);
	\draw[draw=black, thin, solid] (2.50,3.50) rectangle (5.50,-0.50);
	\draw[draw=black, thin, solid] (-1.00,3.00) rectangle (1.00,0.00);
	\draw[draw=black, thin, solid] (3.00,3.00) rectangle (5.00,0.00);
	\draw[draw=black, thin, solid] (-5.50,3.50) rectangle (-2.50,-0.50);
	\node[black, anchor=south west] at (-5.56,3.75) {$p_1$};
	\node[black, anchor=south west] at (-1.56,3.75) {$p_2$};
	\node[black, anchor=south west] at (2.44,3.75) {$p_3$};
	\node[black, anchor=south west] at (-5.06,1.25) {$p_3$};
	\node[black, anchor=south west] at (-5.06,2.25) {$p_2$};
	\node[black, anchor=south west] at (-1.06,2.25) {$p_3$};
	\node[black, anchor=south west] at (-5.06,0.25) {$\hat l_1$};
	\node[black, anchor=south west] at (-1.06,0.25) {$\hat l_2$};
	\node[black, anchor=south west] at (2.94,1.25) {$\hat l_3$};
	\node[black, anchor=south west] at (-1.06,1.25) {$\hat p_4$};
	\node[black, anchor=south west] at (2.94,2.25) {$\hat p_5$};
\end{tikzpicture}
\caption{Przykład raportów o stanie techniki}
\label{fig:raport-ex}
\end{figure}

\begin{figure}[H]\centering
\includegraphics[width=0.8\textwidth]{fig/img/rprt/pat-ex-P.jpg}
\caption{Przykład odniesienia poprzez numer publikacji.}
\end{figure}

\begin{figure}[H]\centering
\includegraphics[width=0.8\textwidth]{fig/img/rprt/pat-ex-A.jpg}
\caption{Przykład odniesienia poprzez numer aplikacji.}
\end{figure}

\begin{figure}[H]\centering
\includegraphics[width=0.8\textwidth]{fig/img/rprt/pat-ex-A-P.jpg}
\caption{Przykład odniesienia poprzez numer aplikacji i publikacji
         jako jeden numer.}
\end{figure}

Powiązywanie cytowań z cytowanymi patentami 
opiera się o zastosowanie wyszukiwania.
Polega ono na wskazaniu sumy zbiorów
słów: numerów patentowych, słów języka naturalnego oraz dat. 
Wskazanie tej sumy zachodzi dla każdej pary wszystkich słów zapytań
ze wszystkimi słowami ze zbioru danych. Dodatkowo zachodzi łączenie
częściowe, które dopasowuje n-gramy poszczególnych słów. Od pewnego
minimalnego dopasowania zostają one uwzględnione. Całość wymaga
pewnych kroków optymalizacyjnych. Głównym jest zasada, że słowa
są dopasowywane tylko pod warunkiem, że zaszło dopasowanie numeru
patentu.

W trakcie wyszukiwania tworzona jest jego punktacja, aby odróżnić
wartościowe wyniki. Oprócz punktacji jest też ustalanie poziomu
wyszukiwania na podstawie tego jakie rzeczywiste dane są łącznikami.
Wybierane są wyłącznie pojedyncze, najlepsze wyniki.

Raporty \ac{PDF} nie posiadają adnotacji tekstowych. 
Znaczy to tyle, że dane są zawarte w sposób czytelny
jedynie dla człowieka i nie są dostępne dla urządzeń w sposób
ustruktoryzowany inny niż ciąg binarny pikseli.
Rozwiązaniem jest proces \ac{OCR}, który obrazy zawierające tekst
przekształca na kod binarny, które można przetwarzać na komputerze
jako ciągi znaków odpowiadające prawdziwemu tekstowi. Pierwszą
czynnością w tym procesie jest zastosowanie pakietu \textit{paddle}.
Zastosowanie modułu pozwala na pozyskanie linijek tekstu z przypisaniem
do ich pozycji. Wynika z tego problem taki, że nie brak jest informacji
o tym gdzie zaczyna i kończy się tekst dotyczący wskazanej obserwacji.
W związku z tym nie sposób jest przypisać tekstu do odpowiednich
wpisów. Dodatkowo dochodzą problemy wynikające z błędów w procesie
skanowania samych dokumentów - zniekształcenia, zaciemnienia, czy
rotacje kartek sprawiają, że proces \ac{OCR} nie jest idealny.
Dodatkowo samo formatowanie nierzadko jest wadliwe co wynika
z wprowadzania danych jeszcze na etapie tworzenia dokumentów.

\subsubsection{Zastosowanie dużego modelu językowego}

Do skutecznego pozyskania danych z dokumentów kluczowe było zastosowanie
dużych modeli językowych z multimodalnymi wejściami. Stan tej technologii
na dzień procesu wyciągania danych był na tyle zaawansowany, że
aspekty techniczne ograniczają się do zastosowania zewnętrznego \ac{API}
dla modelu \textit{openai} \textit{GPT4o}. Model ten w wystarczający
sposób był w stanie przetworzyć obrazy zawierające tekst na ustruktoryzowany
zbiór wpisów tekstowych.

Mimo, że model \textit{paddle} nie dawał wyników pozwalających na
poprawną dalszą analizę to pozwolił na ograniczenie kosztów. Znalezienie
słów kluczowych nagłówków i stopek tabeli z informacjami było wystarczające
aby przyciąć zdjęcia do obszarów zainteresowania.

"""


#lib
import lib.storage, lib.query, lib.flow, prfl
from util import data as D

#calc
import pandas, yaml, tqdm

#plot
import altair as Pt
from util import A4


@lib.flow.placeholder()
def Indexing(storage:lib.storage.Storage, assignpath:str):

  """
  \subsection{Optymalizacja przez indeksowanie}

  Indeksowanie danych z profili, jest wymagane do przeprowadzenia
  wyszukiwania w optymalny komputacyjnie sposób.
  Jest to etap po profilowaniu, który fragmentuje dane na
  ustalone typy: ciągy cyfrowe, daty, słowa kluczowe, n-gramy słów i ciągów.
  W zależności od typu, ilości powtórzeń w danych i ich długości posiadaja
  inne punktacje, które mogą być dalej wykorzystane w procesie wyszukiwania.
  """

  from lib.index import Exact, Words, Digital, Ngrams, Date

  S = storage
  a = assignpath

  with open(a, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  P0, P = Digital(), Ngrams(n=3, suffix=False)
  p = S.melt('number')
  p = P0.add(p)
  p['assignement'] = 'partial-number'
  p = P.add(p)

  D0 = Date()
  d = S.melt('date')
  d = D0.add(d)

  W0, W = Words(), Ngrams(n=3, suffix=False)

  for k in ['name', 'title', 'city']:

    w = S.melt(k)
    w = W0.add(w)
    w['assignement'] = f'partial-{k}'
    w = W.add(w)

  return P0, P, D0, W0, W

@lib.flow.map('cache/rprt/identified.pkl')
@lib.flow.init(qpath='raport.uprp.gov.pl.csv', storage=prfl.repos['UPRP'], docsframe='raw')
def identified(qpath:str, storage:lib.storage.Storage, docsframe:str):

  """
  \subsection{Identyfikacja zapytań}

  Rozpoznawanie zapytań odbywa się w zupełnie innym kontekście i
  nie zwraca dla zapytań informacji o tym skąd pochodzą.
  Identyfikacja korzysta z nazw plików i metadanych samych zapytań
  to dopasowania ich do odpowiednich danych w zbiorze.
  """

  Q = pandas.read_csv(qpath)
  D = storage.data[docsframe]

  assert 'P' in Q.columns
  assert 'filename' in D.columns

  D['P'] = D['filename'].str.extract(r'(\d{6}).*\.xml')
  Q['P'] = Q['P'].astype(str)
  Q = Q.set_index('P')
  D = D.reset_index().set_index('P')

  Y = Q.join(D['doc'], how='left').reset_index()
  if Y['doc'].isna().any(): raise ValueError()

  Y = Y.drop(columns=['P'])

  return Y

@lib.flow.map('cache/rprt/queries.pkl')
@lib.flow.init(identified)
def queries(searches: pandas.Series):

  """
  \subsection{Parsowanie zapytań}

  Parsowanie zapytań to proces wyciągania z tekstów
  ciągów przypominających daty i numery patentowe.
  Proces polega na wstępnym podzieleniu całego napisu na
  części spełniające określone wyrażenia regularne. Później,
  te są łączone na podstawie tego czy w ich pobliżu są oczekiwane
  ciągi takie jak ciągi liczbowe albo skrótowce takie jak "PL".
  """

  Q = searches
  Y, P = [], []

  for i, q0 in Q.iterrows():

    q = lib.query.Query.Parse(q0['query'])

    P.extend([{ 'entrydoc': q0['doc'], 'entry': i, **v } for v in q.codes])

    Y.extend([{ 'entrydoc': q0['doc'], 'entry': i, 'value': v, 'kind': 'number' } for v in q.numbers])
    Y.extend([{ 'entrydoc': q0['doc'], 'entry': i, 'value': v, 'kind': 'date' } for v in q.dates])
    Y.extend([{ 'entrydoc': q0['doc'], 'entry': i, 'value': v, 'kind': 'year' } for v in q.years])
    Y.extend([{ 'entrydoc': q0['doc'], 'entry': i, 'value': v, 'kind': 'word' } for v in q.words])

  return pandas.DataFrame(Y).set_index('entry'),\
         pandas.DataFrame(P).set_index('entry')

class Search:

  "Klasa z metodami pomocniczymi dla wyszukiwania."

  Levels = pandas.CategoricalDtype([
    "weakest", "dated", "partial",
    "supported", "partial-supported",
    "exact", "dated-supported",
    "partial-dated", "partial-dated-supported",
    "exact-supported",
    "exact-dated", "exact-dated-supported"
  ], ordered=True)

  def score(matches):

    "Zwraca ramkę z kolumnami 'score' i 'level' i indeksem 'doc' i 'entry'."

    import cudf

    X: cudf.DataFrame = matches
    S = cudf.DataFrame(index=X.index)

    for k0 in ['number', 'date', 'partial-number']:
      K = [k for k in X.columns if k[3] == k0]
      if not K: continue
      S[k0] = X.loc[:, K].max(axis=1)

    for k0 in ['name', 'city', 'title']:
      K = [k for k in X.columns if k[3] == k0]
      if not K: continue
      S[k0] = X.loc[:, K].sum(axis=1)

    S = S.reindex(columns=['number', 'date', 'partial-number', 'name', 'title', 'city'], fill_value=0)

    L = cudf.DataFrame(index=S.index)
    L['exact'] = S['number'] >= 1
    L['partial'] = S['partial-number'] > 0
    L['dated'] = S['date'] >= 1
    L['supported'] = S['title'] + S['name'] + S['city'] + \
      (S['title'] * S['name'] > 1.) + \
      (S['name']  * S['city'] > 1.) + \
      (S['title'] * S['city'] > 1.) >= 3.

    L0 = Search.Levels.categories
    L['level'] = L0[0]
    for c in L0[1:]:
      try: q = L[c.split('-')].all(axis=1)
      except KeyError: continue
      R = L['level'].where(~ q, c)
      L['level'] = R

    Z = cudf.DataFrame({ 'score': S.sum(axis=1), 
                         'level': L['level'].astype(cudf.CategoricalDtype.from_pandas(Search.Levels)) }, 
                        index=S.index)

    Y = Z.reset_index().sort_values(['level', 'score'])
    Y = Y.groupby('entry').tail(1).set_index(['doc', 'entry'])

    return Y

@lib.flow.placeholder()
def Narrow(queries:pandas.Series, indexes:tuple, pbatch:int=2**14, ngram=True):

  """
  \subsection{Wąskie wyszukiwanie}

  Wyszukiwanie w zależności od parametrów korzysta z dopasowania
  kodami patentowymi albo ich częściami. Później w grafie takich
  połączeń szuka dodatkowych dowodów na istnienie połączenia:
  wspólnych kluczy (np. imion i nazw miast) oraz dat.
  """

  import cudf

  Q0, _ = queries
  P0, P, D0, W0, W = indexes

  QP = Q0.query('kind == "number"')

  mP0 = P0.match(QP['value'], 'max').reset_index()
  mP0['entry'] = mP0['entry'].astype('int64')

  b = pbatch#parial
  if b is not None:

    mP = cudf.concat([P.match(QP.iloc[i:i+b]['value'], 'max', 0.751, ownermatch=mP0)
      for i in tqdm.tqdm(range(0, QP.shape[0], b))]).reset_index()

    m0P = cudf.concat([mP0, mP]).set_index('entry')

  else:
    m0P = mP0.set_index('entry')

  Q = m0P.join(cudf.from_pandas(Q0.astype(str)))\
  [['doc', 'value', 'kind']].to_pandas()#indeksy wymagają inputu pandas

  D0 = D0.extend('doc')
  mD0 = D0.match(Q[Q['kind'] == 'date'][['value', 'doc']], 'max').reset_index()
  if not mD0.empty: mD0['entry'] = mD0['entry'].astype('int64')

  M = cudf.concat([m0P.reset_index(), mD0]).pivot_table(
    index=['doc', 'entry'],
    columns=['repo', 'frame', 'col', 'assignement'],
    values='score', aggfunc='max') if not mP0.empty else cudf.DataFrame()

  W0 = W0.extend('doc')
  mW0 = W0.match(Q[Q['kind'] == 'word'][['value', 'doc']], 'sum').reset_index()
  mW0['entry'] = mW0['entry'].astype('int64')

  if ngram:
    W = W.extend('doc')
    mW = W.match(Q[Q['kind'] == 'word'][['value', 'doc']], 'sum', ownermatch=mW0)\
    .reset_index()

    mW0W = cudf.concat([mW0, mW])

  else:
    mW0W = mW0

  Ms = mW0W.pivot_table(
    index=['doc', 'entry'],
    columns=['repo', 'frame', 'col', 'assignement'],
    values='score',
    aggfunc='sum') if not mW0.empty else cudf.DataFrame()

  if not M.empty and not Ms.empty:
    J = M.join(Ms).fillna(0.0)
  elif not M.empty:
    J = M.fillna(0.0)
  elif not Ms.empty:
    J = Ms.fillna(0.0)
  else:
    return cudf.DataFrame()

  L = Search.score(J)
  L.columns = cudf.MultiIndex.from_tuples([('', '', '', 'score'), ('', '', '', 'level')])

  Y = J[J.index.isin(L.index)].join(L)
  Y = Y[Y[('', '', '', 'level')] >= "partial-supported"]

  Y = Y.reset_index().set_index(('entry', '', '', ''))

  E = Q0['entrydoc'].reset_index().drop_duplicates().set_index('entry')
  E = cudf.Series.from_pandas(E['entrydoc'])

  Y = Y.join(E, how='left')
  Y = Y.set_index(['entrydoc', ('doc', '', '', '')], append=True)
  Y.index.names = ['entry', 'entrydoc', 'doc']
  Y = Y.sort_index()
  Y.columns = cudf.MultiIndex.from_tuples(Y.columns)
  Y = Y[Y.columns[::-1]]

  return Y.to_pandas()

@lib.flow.placeholder()
def Family(queries:pandas.Series, matches:pandas.DataFrame, storage:lib.storage.Storage, assignpath:str):

  "Podmienia kody w zapytaniach na te znalezione w rodzinie patentowej."

  Q, _ = queries
  M = matches
  S = storage

  with open(assignpath, 'r') as f:
    S.assignement = yaml.load(f, Loader=yaml.FullLoader)

  A = S.melt('family-number').reset_index()[['id', 'doc', 'value']]
  B = S.melt('family-jurisdiction').reset_index()[['id', 'doc', 'value']]\
        .rename(columns={'value': 'jurisdiction'})

  U = S.melt('family')[['doc', 'value']]
  U['jurisdiction'] = U['value'].str.extract(r'^(\D+)')
  U['value'] = U['value'].str.extract(r'(\d+)')

  C = pandas.DataFrame()
  if (not A.empty):
    C = A.merge(B, on=['id', 'doc']).drop(columns=['id'])

  C = pandas.concat([X for X in [C, U] if not X.empty]).set_index('doc')
  C = C.query('jurisdiction == "PL"').drop(columns='jurisdiction')

  M = M.index.to_frame().reset_index(drop=True).drop_duplicates()
  P = M.set_index('doc').join(C['value'], how='inner')
  P = P.set_index('entry').rename(columns={"doc_number": "value"})
  P['kind'] = 'number'

  Q = Q[ Q.index.isin(P.index) ].query('kind != "number"')

  Z = []
  for i, q0 in P['value'].items():
    Z.extend([{ 'entry': i, **v } for v in lib.query.Query.Parse("PL"+q0).codes])

  Z = pandas.DataFrame(Z).set_index('entry')

  return pandas.concat([Q, P]).sort_index(), Z



@lib.flow.placeholder()
def Drop(queries:pandas.Series, matches:list[pandas.DataFrame]):

  "Usuwa z wyników zapytań te, które już zostały dopasowane w zadowalający sposób."

  Q, P = queries
  M = matches
  K = [('entry', '', '', ''), ('doc', '', '', ''),
        ('', '', '', 'level'), ('', '', '', 'score')]

  Y = []

  for m in M:
    y = m.reset_index()[K]
    y.columns = ['entry', 'doc', 'level', 'score']
    Y.append(y)

  Y = pandas.concat(Y, axis=0, ignore_index=True)
  assert Y['level'].dtype == 'category'
  Y = Y.sort_values(['level', 'score'])
  Y['level'] = Y['level'].astype(Search.Levels)
  Y = Y[Y['level'] >= "partial-dated-supported"]

  p0 = P.index.astype(str).unique()
  p = p0[p0.isin(Y['entry'])]

  q0 = Q.index.astype(str).unique()
  q = q0[q0.isin(Y['entry'])]

  return Q[ ~ Q.index.isin(q)], P[ ~ P.index.isin(p) ]

flow = { k: dict() for k in D.keys() }

for k, p in D.items():

  flow[k]['index'] = Indexing(prfl.repos[k], assignpath=p+'/assignement.yaml').map(p+'/indexes.pkl')

  flow[k]['narrow'] = Narrow(queries, flow[k]['index'], pbatch=2**14).map(p+'/narrow.pkl')

flow['UPRP']['narrow'] = Narrow(queries, flow['UPRP']['index'], pbatch=2**13).map(D['UPRP']+'/narrow.pkl')

flow['USPG']['narrow'] = Narrow(queries, flow['USPG']['index'], pbatch=2**14).map(D['USPG']+'/narrow.pkl')

flow['USPA']['narrow'] = Narrow(queries, flow['USPA']['index'], pbatch=2**14).map(D['USPA']+'/narrow.pkl')

flow['Lens']['narrow'] = Narrow(queries, flow['Lens']['index'], pbatch=2**12).map(D["Lens"]+'/narrow.pkl')

drop0 = Drop(queries, [flow['UPRP']['narrow'], flow['Lens']['narrow']]).map('alien.base.pkl')

for k, p in D.items():

  flow[k]['drop'] = Drop(queries, [flow[k]['narrow']]).map(p+'/alien.pkl')

flow['Google']['narrow'] = Narrow(drop0, flow['Google']['index'], pbatch=2**10).map(D["Google"]+'/narrow.pkl')

for k0 in ['Lens', 'Google']:

  k = f'UPRP-{k0}'
  flow[k] = dict()
  p = f'{D["UPRP"]}/{D[k0]}'
  D[k] = p

  flow[k] = dict()

  flow[k]['query'] = Family(queries=queries, matches=flow[k0]['narrow'], storage=prfl.repos[k0], assignpath=D[k0]+'/assignement.yaml').map(p+'/family.pkl')

  flow[k]['narrow'] = Narrow(queries=flow[k]['query'], indexes=flow['UPRP']['index'], pbatch=None, ngram=False).map(D[k]+'/narrow.pkl')

drop = Drop(queries, [flow[k]['narrow'] for k in D.keys()]).map('alien.pkl')


@lib.flow.map('cache/rprt/edges.pkl')
@lib.flow.init(flow['UPRP']['narrow'])
def edges(results:pandas.DataFrame):

  r"""
  \subsection{Kryterium wyszukiwania}

  \TODO{opisać}
  """

  X = results

  Y = pandas.DataFrame({'to': X.index.get_level_values('entrydoc'),
                        'from': X.index.get_level_values('doc'),
                        'level': X[('', '', '', 'level')],
                        'score': X[('', '', '', 'score')]})
  return Y

@lib.flow.map('fig/rprt/UPRP-score.pdf')
@lib.flow.init(edges)
def UPRPscoreplot(results=pandas.DataFrame):

  r"""
  \subsection{Wyniki wyszukiwania}

  \chart{fig/rprt/UPRP-score.pdf}
  { Wykres punktacji wyszukiwania patentów 
    z cytowań w raportach o stanie techniki. }
  """

 #dane
  R = results

  R = R.reset_index()
  R['large'] = R['score'].apply(lambda x: '>200' if x > 200 else '≤200')

  R.level = R.level.str.replace('partial', 'częśc.')
  R.level = R.level.str.replace('supported', 'popart.')
  R.level = R.level.str.replace('dated', 'zgodne')
  R.level = R.level.str.replace('exact', 'dokł.')

  pR = Pt.Chart(R)

 #osie
  xD = Pt.X('score:Q').title('Rozkład gęstości punktacji')
  yD = Pt.Y('density:Q').title(None)

  xF200 = Pt.X('large').title(None)
  yF200 = Pt.Y('count(large)').title(None)

  xL = Pt.X('level').title(None)
  yL = Pt.Y('count(level)').title(None)

 #wykresy
  D = pR.mark_area().encode(xD, yD)
  D = D.transform_filter(Pt.datum.large == '≤200')
  D = D.transform_density('score', as_=['score', 'density'])

  F200 = pR.mark_bar().encode(xF200, yF200)

  L = pR.mark_bar().encode(xL, yL)

 #układ
  D = D.properties(width=0.4*A4.W, height=0.05*A4.H)
  F200 = F200.properties(width=0.04*A4.W, height=0.05*A4.H)
  L = L.properties(width=0.5*A4.W, height=0.05*A4.H)

  return (F200|D)&L