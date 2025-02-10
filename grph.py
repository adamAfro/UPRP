#lib
import lib.flow, gloc, endo, raport as rprt

#calc
import pandas, altair as Pt, networkx as nx

#plot
import altair as Pt
from util import A4

@lib.flow.make()
def network(docrefs:pandas.DataFrame, 
            docsign:pandas.DataFrame,
            dist:pandas.DataFrame,
            spatial:list[str],
            temporal:list[str],
            Jsim=[], feats=[],
            borders=None):

  """
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
  N = N[['id', 'doc']+K]

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
  for k in temporal:
    E[f't{k}'] = (E[f'{k}Y'] - E[k]).dt.days.astype(int)
    E = E.drop(columns=[f'{k}Y'])

 #poprawka na błędne dane
  E = E[E['tapplication'] > 0]

 #Jaccard
  JX = E.apply(lambda x: frozenset([k for k in Jsim if x[k] > 0]), axis=1)
  JY = E.apply(lambda x: frozenset([k[:-1] for k in [f'{k0}Y' for k0 in Jsim] if x[k] > 0]), axis=1)
  E['Jaccard'] = JX.to_frame().join(JY.rename(1)).apply(lambda x: len(x[0] & x[1]) / len(x[0] | x[1]), axis=1)
  E = E.drop(columns=Jsim+[f'{k}Y' for k in Jsim])

  E = E.drop(columns=feats+[f'{k}Y' for k in feats])

 #komponenty
  G = nx.Graph()
  G.add_edges_from(E[['id', 'idY']].values)
  C0 = list(nx.connected_components(G))
  C = pandas.DataFrame([x for x in zip(range(len(C0)), C0)])[[1]]
  C['len'] = C[1].apply(len)
  C = C.sort_values('len').reset_index(drop=True).reset_index().explode(1)
  C.columns = ['component', 'id', 'csize']
  C['component'] = C['component'].map(lambda x: f'C{x}')
  N = N.set_index('id').join(C.set_index('id')).reset_index()

  def carto():

    v0 = Pt.Chart(pandas.Series({ 'izolowane': N['component'].isna().sum(), 
                                 'w składowych': (~N['component'].isna()).sum() }).rename('count').reset_index()).mark_bar()
    v0 = v0.encode(Pt.Y('index:N').title(''))
    v0 = v0.encode(Pt.X('count:Q').title('Ilość osób'))

    v = Pt.Chart(N['component'].value_counts().reset_index()).mark_area()
    v = v.transform_density('count', as_=['count', 'density'])
    v = v.encode(Pt.X('count:Q').title('Ilość składowych'))
    v = v.encode(Pt.Y('density:Q').title('Gęstość'))

    vj = Pt.Chart(N['component'].value_counts().reset_index()).mark_circle(size=1)
    vj = vj.transform_calculate(jitter='sqrt(-2*log(random()))*cos(2*PI*random())')
    vj = vj.encode(Pt.X('count:Q').title(None))
    vj = vj.encode(Pt.Y('jitter:Q').axis(None).title(None))
    vj = vj.encode(Pt.Opacity('count:Q').scale(range=[0.5, 1]).legend(None))

    d = Pt.Chart(E).mark_circle()
    d = d.encode(Pt.X('distance:Q').title('Dystans').bin(step=100))
    d = d.encode(Pt.Y('tapplication:Q').title('Opóźnienie').bin(step=365))
    d = d.encode(Pt.Size('count()')\
                   .title('Ilość patentów')\
                   .legend(orient='top', columns=2))

    EGX = E.groupby(spatial).agg({ 'id': 'size', 'distance': 'mean' })
    mX = Pt.Chart(EGX).mark_circle().project('mercator')
    mX = mX.encode(Pt.Latitude(spatial[0], type='quantitative'))
    mX = mX.encode(Pt.Longitude(spatial[1], type='quantitative'))
    mX = mX.encode(Pt.Color('distance', type='quantitative')\
                    .legend(orient='top')\
                    .title(['Średni dystans od osób ref.'])\
                    .scale(range=['yellow', 'red', 'blue']))
    mX = mX.encode(Pt.Size('id:Q'))

    EGY = E.groupby([f'{k}Y' for k in spatial]).agg({ 'id': 'size', 'distance': 'mean' })
    EGY.index.names = spatial
    mY = Pt.Chart(EGY).mark_circle().project('mercator')
    mY = mY.encode(Pt.Latitude(spatial[0], type='quantitative'))
    mY = mY.encode(Pt.Longitude(spatial[1], type='quantitative'))
    mY = mY.encode(Pt.Color('distance', type='quantitative'))
    mY = mY.encode(Pt.Size('id:Q'))

    ED = (EGX - EGY)[['id']].reset_index()
    ED['minus'] = (ED['id'] < 0).replace({ True: 'odpływ', False: 'dopływ' })
    ED['id'] = ED['id'].abs()
    mD = Pt.Chart(ED).mark_circle().project('mercator')
    mD = mD.encode(Pt.Latitude(spatial[0], type='quantitative'))
    mD = mD.encode(Pt.Longitude(spatial[1], type='quantitative'))
    mD = mD.encode(Pt.Color('minus', type='nominal')\
                    .legend(orient='bottom')\
                    .title(['Dominujący kierunek przepływu'])\
                    .scale(range=['red', 'blue']))
    mD = mD.encode(Pt.Size('id:Q')\
                     .title('Ilość referowanych osób')\
                     .legend(orient='bottom', columns=3))

    d = d.properties(width=0.1*A4.W, height=0.2*A4.W)
    v0 = v0.properties(width=0.1*A4.W, height=0.05*A4.W)
    v = v.properties(width=0.1*A4.W, height=0.05*A4.W)
    vj = vj.properties(width=0.1*A4.W, height=0.05*A4.W)
    mX = mX.properties(width=0.1*A4.W, height=0.1*A4.W)
    mY = mY.properties(width=0.1*A4.W, height=0.1*A4.W)
    mD = mD.properties(width=0.25*A4.W, height=0.25*A4.W)
    if borders is not None:
      mX = Pt.Chart(borders).mark_geoshape(fill='black') + mX
      mY = Pt.Chart(borders).mark_geoshape(fill='black') + mY
      mD = Pt.Chart(borders).mark_geoshape(fill='black') + mD

    return (((mX | mY).resolve_scale(color='shared') & mD) | (d & v0 & vj & v)).resolve_scale(size='independent')

  carto()

  return E, N, carto()

web = network(rprt.valid, endo.data, gloc.dist,
              spatial=['lat', 'lon'], temporal=['grant', 'application'],
              Jsim=[f'clsf-{l}' for l in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']],
              feats=['entity', 'pgid', 'wgid'], borders=gloc.region[0])

web = web.map(('cache/grph/edges.pkl', 'cache/grph/nodes.pkl', 'fig/grph/M.png'))

FLOW = dict(web=web)