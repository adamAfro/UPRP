#lib
import lib.flow, gloc, rprt, subj, util

#calc
import pandas, altair as Pt, networkx as nx

#plot
import altair as Pt
from util import A4


@lib.flow.map(('cache/grph/edges.pkl', 'cache/grph/nodes.pkl'))
@lib.flow.init(rprt.edges, subj.mapped, gloc.dist,
               spatial=['lat', 'lon'], temporal=['grant', 'application'],
               Jsim=[f'clsf-{l}' for l in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']],
               feats=['entity', 'pgid', 'wgid'])
def network(docrefs:pandas.DataFrame, 
            docsign:pandas.DataFrame,
            dist:pandas.DataFrame,
            spatial:list[str],
            temporal:list[str],
            Jsim=[], feats=[]):

  r"""
  \subsubsection
  {Tworzenie grafu na podstawie raportów o stanie techniki}

  Graf $G$ jest grafem skierowanym o krawędziach $E$ i węzłach
  będących osobami pełniącymi role patentowe.

  Jest stworzony na podstawie raportów o stanie techniki
  składa się z 2 rodzajów węzłów: patentów i osób pełniących 
  role patentowe.

  Pierwszym etapem jest utworzenie krawędzi $E_r$ między dokumentami.
  Każdy raport dotyczy jednego patentu, a może odwoływać się 
  do wielu innych. Odwołanie umieszczone w raporcie do innego
  patentu traktujemy jako krawędź w grafie skierowanym $G_r$.
  Kierunek grafu jest zgodny z przepływem informacji, co znaczy,
  że patent którego dotyczy raport jest węzłem końcowym,
  a wszystkie patenty wymienione w raporcie są węzłami początkowymi.

  Drugim etapem jest utworzenie krawędzi $E_x$ między osobami,
  a dokumentami. Najpierw krawędzie skierowane są tworzone 
  w kierunku dokumentów, które były wymieniane w raportach.
  Następnie krawędzie $E_y$ z patentów będących przedmiotami 
  raportów są tworzone w kierunku osób, które są ich autorami.

  Ostatecznie krawędzie $E$ grafu powstają jeśli istnieje ściezka
  między 2 wierzchołkami reprezentującymi osoby. Zgodnie z kierunkiem
  grafu, te krawędzie są skierowane, a ich zwrot reprezentuje kierunek
  przepływu wiedzy.
  """

  assert { 'application' }.issubset(docsign.columns), docsign['application']

  R = docrefs
  N = docsign
  D = dist

  K = spatial + temporal + Jsim + feats
  N = N[['id', 'doc']+K].copy()
  N['year'] = N['application'].dt.year.astype(int)

 #graf
  R['edge'] = range(len(R))

  EX = N.set_index('doc').join(R.set_index('from'), how='inner')
  EY = N.set_index('doc').join(R.set_index('to'), how='inner')

  E = EX.reset_index().set_index('edge')
  E = E.join(EY.set_index('edge'), rsuffix='Y', how='inner')
  E = E.reset_index().drop(columns=['edge'])

 #dystans
  Ks = spatial + [f'{k}Y' for k in spatial]
  E = E.set_index(Ks)
  D = D.reset_index().melt(id_vars=[(spatial[0], ''), 
                                    (spatial[1], '')], value_name='distance')
  D.columns = Ks + ['distance']
  D = D.set_index(Ks)
  E = E.join(D).reset_index()

 #czas
  N['year'] = N['application'].dt.year.astype(int)
  E['year'] = E['application'].dt.year.astype(int)
  E['yearY'] = E['applicationY'].dt.year.astype(int)
  for k in temporal:
    E[f't{k}'] = (E[f'{k}Y'] - E[k]).dt.days.astype(int)

 #poprawka na błędne dane
  E = E[E['tapplication'] > 0]

 #Jaccard
  JX = E.apply(lambda x: frozenset([k for k in Jsim if x[k] > 0]), axis=1)
  JY = E.apply(lambda x: frozenset([k[:-1] for k in [f'{k0}Y' for k0 in Jsim] if x[k] > 0]), axis=1)
  E['Jaccard'] = JX.to_frame().join(JY.rename(1)).apply(lambda x: len(x[0] & x[1]) / len(x[0] | x[1]), axis=1)
  E = E.drop(columns=Jsim+[f'{k}Y' for k in Jsim])

  E = E.query('yearY >= 2011')

  return E, N

@lib.flow.map(('fig/grph/M-dist.pdf'))
@lib.flow.init(network[0], borders=gloc.region[1])
def distcart(edges, borders, spatial=['lat', 'lon']):

  r"""
  \subsection{Lokalizacje osób cytujących i cytowanych}

    \newpage
  \begin{multicols}{2}
  \chart{fig/grph/M-dist.pdf}
  { Mapy z lokalizacjami osób cytujących i cytowanych }
  \columnbreak
  Na mapach obok zaprezentowane są lokalizacje osób cytujących
  oraz cytowanych. Kolor punktów odpowiada średniej odległości
  między osobą a osobą referującą (żółty --- bliskie, czerwony --- dalekie).
  Rozmiar punktów odpowiada ilości referowanych osób:
  na wykresie górnym dotyczy osób cytujących, a na dolnym --- cytowanych.
  Ostatnia mapa przedstawia różnicę między ilością osób cytujących
  i cytowanych w danej lokalizacji. Kolor punktów odpowiada dominującemu
  kierunkowi przepływu wiedzy, gdzie \textit{odpływ} oraz \textit{dopływ}
  rozumiemy jako, odpowiednio, przewagę osób cytujących albo cytowanych.
  Należy wyróżnić lokalizacje dużych miast w Polsce, jako czołowe
  w przepływie wiedzy --- mowa najczęściej o stolicach województwa.
  \end{multicols}

  Mapy pokazują dużą dysproporcję między południem, a północą ---
  południe jest znacząco bardziej aktywne w przepływie wiedzy.
  Na północy istnieją jedynie 2 ośrodki: Trójmiasto oraz Szczecin.
  Na południu charakterystycznie dużo osób referuje się wzajemnie,
  a wyróżnić można miasta: Warszawa, Poznań, Kraków, 
  Katowice, Kielce, Wrocław oraz Lublin.
  """

  E = edges

  X = E.groupby(spatial)
  X = X.agg({ 'id': 'size', 'distance': 'mean' })
  X = X.reset_index()
  mX = Pt.Chart(X).mark_circle().project('mercator')
  mX = mX.encode(Pt.Latitude(spatial[0], type='quantitative'))
  mX = mX.encode(Pt.Longitude(spatial[1], type='quantitative'))
  mX = mX.encode(Pt.Color('distance', type='quantitative')\
                  .legend(orient='right')\
                  .title('Śr. dys. do os.~referującej'.split('~'))\
                  .scale(range=['yellow', 'red', 'blue']))
  mX = mX.encode(Pt.Size('id:Q'))

  Y = E.groupby([f'{k}Y' for k in spatial])
  Y = Y.agg({ 'id': 'size', 'distance': 'mean' })
  Y.index.names = spatial
  Y = Y.reset_index()
  mY = Pt.Chart(Y).mark_circle().project('mercator')
  mY = mY.encode(Pt.Latitude(spatial[0], type='quantitative'))
  mY = mY.encode(Pt.Longitude(spatial[1], type='quantitative'))
  mY = mY.encode(Pt.Color('distance', type='quantitative')\
                  .legend(orient='right')\
                  .title('Śr. dys. od os.~referowanej'.split('~'))\
                  .scale(range=['yellow', 'red', 'blue']))
  mY = mY.encode(Pt.Size('id:Q'))

  X = X.set_index(spatial)
  Y = Y.set_index(spatial)
  D = (X - Y)[['id']].reset_index()
  D['minus'] = (D['id'] < 0).replace({ True: 'odpływ', False: 'dopływ' })
  D['id'] = D['id'].abs()
  mD = Pt.Chart(D).mark_circle().project('mercator')
  mD = mD.encode(Pt.Latitude(spatial[0], type='quantitative'))
  mD = mD.encode(Pt.Longitude(spatial[1], type='quantitative'))
  mD = mD.encode(Pt.Color('minus', type='nominal')\
                  .legend(orient='right')\
                  .title('Dominujący kierunek przepływu'.split(' '))\
                  .scale(range=['red', 'blue']))
  mD = mD.encode(Pt.Size('id:Q')\
                    .title('Ilość referowanych osób')\
                    .legend(orient='bottom', columns=4,
                            values=[50, 100, 200, 500,
                                    1000, 2000, 3000, 5000,
                                    10000, 15000]))

  mX = mX.properties(width=0.4*A4.W, height=0.25*A4.W)
  mY = mY.properties(width=0.4*A4.W, height=0.25*A4.W)
  mD = mD.properties(width=0.4*A4.W, height=0.25*A4.W)
  if borders is not None:
    mX = Pt.Chart(borders).mark_geoshape(fill='black') + mX
    mY = Pt.Chart(borders).mark_geoshape(fill='black') + mY
    mD = Pt.Chart(borders).mark_geoshape(fill='black') + mD

  return (mX & mY).resolve_scale(color='shared') & mD

@lib.flow.map(('fig/grph/F-dist.pdf'))
@lib.flow.init(network[0])
def distplot(edges:pandas.DataFrame):

  r"""
  \subsection{Zasięg}

  \chart{fig/grph/F-dist.pdf}
  { Wykresy rozkładu odległości między osobami cytującymi, a cytowanymi }

  Powyższe wykresy przedstawiają rozkład odległości 
  między osobami cytującymi, a cytowanymi. 
  Wykresy słupkowe prezentują
  histogram wartości niezerowych po prawej, 
  oraz porównanie wartości zerowych i niezerowych po lewej.
  Obserwujemy znaczącą frakcję wartości zerowych, 
  które stanowią ponad 1/3 wszystkich odległości.
  Należy z tego wnosić, że znaczna część cytowań
  zachodzi między osobami z tej samej lokalizacji.
  Histogram zawiera także zaznaczone wartości statystyk pozycyjnych
  Średnia jest w dużej dysproporcji w stosunku do mediany
  co wskazuje na skośność rozkładu.
  Wartości w przedziale 260-280 są charakterystycznie częste.
  Wykres gęstości poniżej wskazuje na dwie wyraźne grupy
  odległości, jedną wokół 10, a drugą wokół 270.
  Należy także zaznaczyć długi ogon rozkładu ---
  większe wartości są coraz rzadsze 
  w miarę zwiększania ich wartości.
  """

  E = edges
  E.distance = E.distance.astype(float)
  E = E[['distance']].copy()

 #obliczenia
  is0 = lambda d: '= 0' if d == 0 else '> 0'
  E['distzero'] = E.distance.apply(is0)

  S = E.distance.describe().drop('count')
  S = S.rename(util.Translation.describe).reset_index()
  S['label'] = S['index'] + ': ' + S.distance.round(2).astype(str)

 #dane
  pS = Pt.Chart(S)
  pE = Pt.Chart(E)

 #osie
  xF0 = Pt.X('distzero:N')
  yF0 = Pt.Y('count(distzero):Q')
  cF0 = Pt.Color('distzero:N')
  cF0 = cF0.scale(range=['black', '#4c78a8']).legend(None)

  pF = pE.transform_filter((Pt.datum.distzero == '> 0'))
  xF = Pt.X('distance').bin(Pt.Bin(maxbins=50))
  xF = xF.axis(labelAngle=270)
  yF = Pt.Y('count(distance)')

  xD = Pt.X('value:Q')
  yD = Pt.Y('density:Q')

  xL = Pt.X('distance')
  cL = Pt.Color('label').scale(scheme='category10')
  cL = cL.legend(orient='top', columns=2)

 #tytuły
  for v in [xF0, yF0, cF0, xF, yF, xD, yD, xL, cL]: v.title = None
  yF = yF.axis(orient='right').title('Liczność bez zerowych')
  cL = cL.title('Statystyki (włączajac zerowe)')
  xD = xD.title('Gęstość rozkładu odległości między osobą cytującą, a cytowaną')

 #wykresy
  F0 = pE.mark_bar().encode(xF0, yF0, cF0)
  F = pF.mark_bar().encode(xF, yF)
  D = pF.mark_area().encode(xD, yD)
  D = D.transform_density(density='distance')
  L = pS.mark_rule(size=0.005*A4.W, strokeDash=[4,4]).encode(xL, cL)

 #układ
  F0 = F0.properties(width=0.1*A4.W, height=0.25*A4.W)
  F = F.properties(width=0.8*A4.W, height=0.25*A4.W)
  D = D.properties(width=1.0*A4.W, height=0.05*A4.W)
  p = (F0 | (L + F).resolve_scale(color='independent')) & D

  return p

@lib.flow.map(('fig/grph/F-dist-yearly.pdf'))
@lib.flow.init(network[0])
def distplotyear(edges:pandas.DataFrame):

  r"""
  \subsection{Zasięg}

  \begin{multicols}{2}
  \chart{fig/grph/F-dist-yearly.pdf}
  { Rozkład odległości między osobami cytującymi, a cytowanymi }
  \end{multicols}
  """

  E = edges[[ 'yearY', 'distance' ]]
  E = E.query('yearY >= 2011')

  pC = Pt.Chart(E)
  pD = Pt.Chart(E)

  xC = Pt.X('yearY:O').title(None)
  yC = Pt.Y('count(yearY):Q').title(None)
  C = pC.mark_bar().encode(xC, yC)

  xD = Pt.X('distance:Q').title(None)
  yD = Pt.Y('density:Q').title(None)
  fD = Pt.Row('yearY:O').title(None)

  yC = yC.title('Liczność')
  xC = xC.title('Rok')
  xD = xD.title('Gęstość odległości między osobami~cytującymi, a cytowanymi'.split('~'))

  D = pD.encode(xD, yD, fD).mark_area()
  D = D.transform_density('distance', as_=['distance', 'density'], groupby=['yearY'])

  C = C.properties(width=0.4*A4.W, height=0.05*A4.H)
  D = D.properties(width=0.4*A4.W, height=0.05*A4.H)
  D = D.resolve_axis(x='shared', y='shared')
  p = (C & D).resolve_scale(y='independent')

  return p

@lib.flow.map(('fig/grph/F-delay.pdf'))
@lib.flow.init(network[0])
def delayplot(): raise NotImplementedError()

@lib.flow.map(('fig/grph/M-delay.pdf'))
@lib.flow.init(network[0])
def delaycart(): raise NotImplementedError()