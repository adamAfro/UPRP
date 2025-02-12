#lib
import lib.flow, gloc, endo, raport as rprt

#calc
import pandas, altair as Pt, networkx as nx, numpy, statsmodels.api as sm
from pandas import DataFrame as DF

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

 #poprawka na błędne dane
  E = E[E['tapplication'] > 0]

 #Jaccard
  JX = E.apply(lambda x: frozenset([k for k in Jsim if x[k] > 0]), axis=1)
  JY = E.apply(lambda x: frozenset([k[:-1] for k in [f'{k0}Y' for k0 in Jsim] if x[k] > 0]), axis=1)
  E['Jaccard'] = JX.to_frame().join(JY.rename(1)).apply(lambda x: len(x[0] & x[1]) / len(x[0] | x[1]), axis=1)
  E = E.drop(columns=Jsim+[f'{k}Y' for k in Jsim])

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

    dt = Pt.Chart(E.assign(year=E['application'].dt.year.astype(int))).mark_circle()
    dt = dt.encode(Pt.X('year:O').title('Rok apl. cytowanego pat.').axis(values=[2006, 2013, 2018, 2022]))
    dt = dt.encode(Pt.Y('tapplication:Q').title('Opóźnienie').bin(step=365))
    dt = dt.encode(Pt.Size('count()')\
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
    dt = dt.properties(width=0.1*A4.W, height=0.2*A4.W)
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

    return (((mX | mY).resolve_scale(color='shared') & mD) | (d & dt) | (v0 & vj & v)).resolve_scale(size='independent')

  return E, N, carto()

@lib.flow.make()
def BDLwojload(path:str, regions:pandas.DataFrame):

  R = regions[['geometry', 'name', 'gid']].copy()
  R['name'] = R['name'].str.upper()
  R = R.set_index('name')

  X = pandas.read_csv(path).drop(columns=['gid'])
  X = X.set_index('region').join(R).reset_index().dropna(subset=['gid'])
  X['key'] = X['subject'] + '; ' + X['section'] + '; ' + X['name'] + '; ' + X['var']

 #table
  T = X.pivot_table(index=['year'], columns=['key', 'gid'], values='value')
  T = T.loc[:, T.loc[2013:2022].notna().all()]

  Y = T.melt(ignore_index=False).reset_index().dropna()
  Y = Y.rename(columns={'gid': 'wgid'})
  Y = Y.pivot_table(index=['year', 'wgid'], columns='key', values='value')

  return Y

@lib.flow.make()
def transfer(edges:pandas.DataFrame, nodes:pandas.DataFrame, spatiotemp:pandas.DataFrame, IPCby=False):

  def addyear(N:DF, E:DF) -> tuple[DF, DF]:

    N['year'] = N['application'].dt.year
    E['year'] = E['application'].dt.year
    E['yearY'] = E['applicationY'].dt.year
    E['delay'] = (E['applicationY'] - E['application']).dt.days

    return N, E

  def edgagg(E:DF, by:list[str]) -> DF:

    G = E.groupby(by)
    E = G.size().rename('transfer').to_frame()
    E = E.join(G.agg({'distance': 'mean',
                      'delay': 'mean'})).reset_index()
    return E

  def nodagg(N:DF, by:list[str]) -> DF:

    N = N.groupby(by).size()
    N = N.rename('size').reset_index()
    # N = N.set_index('wgid').join(N.groupby('wgid')['size'].sum().rename('acum')).reset_index()

    return N

  def addfeats(N:DF, F:DF, by:list[str]) -> DF:

    N = N.set_index(by).join(F, how='right').reset_index()

    return N

  def addnods(E:DF, N:DF, xby:list[str], yby:list[str]) -> DF:

    Nx = N.set_index(xby).copy()
    Ny = Nx.copy().reset_index().add_suffix('Y').set_index(yby)
    E = E.set_index(xby).join(Nx).reset_index()
    E = E.set_index(yby).join(Ny).reset_index()

    return E

  def selfeat(y:pandas.Series, M:DF, keep:list, corrtres:float) -> list[str]:

    M = M.loc[:, (M > 0).all(axis=0) ]
    Cy = M[[k for k in M.columns if k not in keep]].corrwith(y).to_frame()
    Cx = M.corr()
    CxE = Cx > corrtres
    Cg = nx.Graph(CxE)
    C0 = list(nx.connected_components(Cg))
    C0 = [c for c in C0 if not any(k in c for k in keep)]
    C0 = pandas.DataFrame([x for x in zip(range(len(C0)), C0)])[[1]].explode(1)
    C0 = C0.reset_index().set_index(1)
    Cy = Cy.join(C0)
    Cy = Cy.dropna().groupby('index')[0].idxmax()

    return [v for v in Cy.values] + keep

  E = edges

  E['distance'] += numpy.abs(numpy.random.normal(0, 1, E.shape[0]))
  E['distance'] = E['distance'].replace(0, 1)

  N = nodes
  R = spatiotemp
  Kx = ['year', 'wgid']
  Ky = ['yearY', 'wgidY']

  N, E = addyear(N, E)

  N = N.query(f'year >= 2013')
  N = nodagg(N, by=Kx)
  N = addfeats(N, R, by=Kx)

  E = E.query(f'year >= 2013')
  E = edgagg(E, by=Kx + Ky)
  E = addnods(E, N, Kx, Ky)

  E = E.set_index(Kx+Ky)
  F = E.drop(columns=['transfer'])
  F = F[selfeat(E['transfer'], F, keep=['size', 'sizeY', 'distance', 'delay'], corrtres=0.5)]
  E = pandas.concat([E['transfer'], F], axis=1)

  E = E.astype(float)

  return E

Translate = { 'i': 'wejś.',
              'j': 'wyjś.',
              'D': 'dystans',
              'P': 'opóźnienie (śr.)',
              'Pd': 'opóźnienia (rt.)',
              'const': 'stała',
              'N': 'n-patentów (i)',
              'NY': 'n-patentów (j)',
              'T': 'przepływ',
              'pearcorr': 'korelacja Pearsona',
              'VIFC': 'VIF ze stałą',
              'Dependent Variable': 'zm. zależna',
              'Coef.': 'wartość',
              'Adj. R-squared': 'R²a.',
              'No. Observations': 'obserwacje',
              'Df Residuals': 'st.sw.r.',
              'Df Model': 'st.sw.m.',
              'F-statistic': 'F-statystyka',
              'Prob (F-statistic)': 'p-wartość',
              'Log-Likelihood': 'log-wiarygodność',
              'Deviance': 'odchylenie',
              'Pearson chi2': 'χ Pearsona',
              'Pseudo R-squ.': 'R² pseudo',
              'R-squared': 'R²',
              'Log-Likelihood': 'log-wiarygodność',
              'Scale': 'skala',
              'mR': 'śr. reszt',
              'Kurtosis': 'kurtoza',
              'Skewness': 'skośność',
              'Condition No.': 'war. warunk.',
              'lat': 'szerokość geogr.',
              'lon': 'długość geogr.',
              'year': 'rok',
              'wgid': 'województwo',
              'gid': 'id. geogr.',
              'pgid': 'powiat',
              'linr': 'regresja liniowa',
              'zip': 'regresja Zero-Inflated Poisson',
                                                    }

@lib.flow.make()
def regr(model:str, preped:pandas.DataFrame, endo:str):

  r"""
  \newpage
  \subsubsection{Model grawitacyjny}
  """

  IY = [k[:-1] for k in preped.index.names if k.endswith('Y')]
  IY = [Translate.get(k, k) for k in IY]

  I0 = [k for k in preped.index.names if not k.endswith('Y')]
  I0 = [Translate.get(k, k) for k in I0]

  X = preped.reset_index(drop=True)
  assert X.isna().sum().sum() == 0, X.isna().sum()
  assert not (X == 0).any().any(), X.loc[X.apply(lambda x: x == 0, axis=0).any(axis=1), 
                                    X.apply(lambda x: x == 0, axis=0).any(axis=0)]

  X = numpy.log(X)
  K = [k for k in X.columns if k != endo]

  if model == 'linr':
    m = sm.OLS(X[endo], X[K]).fit()
  elif model == 'pois':
    m = sm.Poisson(X[endo], X[K]).fit()
  elif model == 'nelo':
    m = sm.GLM( X[endo], X[K], 
                family=sm.families.NegativeBinomial(link=sm.families.links.log())).fit()
  elif model == 'zip':
    m = sm.ZeroInflatedPoisson(X[endo], X[K]).fit()

  X['__Y__'] = m.predict(X[K])
  X['__e__'] = X[endo] - X['__Y__']

 #predykcje
  d = [min(X[endo].min(), X['__Y__'].min()), max(X[endo].max(), X['__Y__'].max())]

  def plot():

    P = Pt.Chart(X).mark_point()
    P = P.encode(Pt.X(endo).scale(domain=d).title('Log. obserwacji'))
    P = P.encode(Pt.Y('__Y__').scale(domain=d).title('Predykcje'))

   #reszty
    Z = Pt.Chart(X).mark_point()
    Z = Z.encode(Pt.X(endo).scale(domain=d).title('Log. obserwacji'))
    Z = Z.encode(Pt.Y('__e__').title('Reszty'))

   #reszty
   #histogram
    R = Pt.Chart(X).mark_bar()
    R = R.encode(Pt.Y('__e__:Q').axis(None).title(None).bin(step=0.25))
    R = R.encode(Pt.X('count(__e__):Q').title(None))

    mR = X['__e__'].mean()
    mR = Pt.Chart(pandas.DataFrame({'mean': [mR]})).mark_rule(color='red')
    mR = mR.encode(y='mean:Q')

   #model
    s = pandas.concat([m.summary2().tables[0].iloc[:, 0:2].rename(columns={0:'label', 1:'value'}),
                      m.summary2().tables[0].iloc[:, 2:4].rename(columns={2:'label', 3:'value'})] +\
                      ([] if len(m.summary(2).tables) < 3 else \
                      [m.summary2().tables[2].iloc[:, 0:2].rename(columns={0:'label', 1:'value'}),
                      m.summary2().tables[2].iloc[:, 2:4].rename(columns={2:'label', 3:'value'})]))

    s['value'] = pandas.to_numeric(s['value'], errors='coerce')
    s.loc[-1] = ('mR', X['__e__'].mean())
    s = s.dropna()
    s['label'] = s['label'].str.strip(':')
    s['label'] = s['label'].replace(Translate)
    S = Pt.Chart(s).mark_text()
    S = S.encode(Pt.Text('value:N', format='.1f'))
    S = S.encode(Pt.Y('label:N').title(None))

   #współczynniki
    c = m.summary2().tables[1].reset_index()
    c = c.melt(id_vars=['index'])
    c['value'] = c['value'].astype(float)
    c['index'] = c['index'].replace(Translate)
    c['variable'] = c['variable'].replace(Translate)

    c['plus'] = (c['value'] >= 0).replace({True: '+', False: '-'})
    c['value'] = c['value'].abs()
    C = Pt.Chart(c.query('variable == "wartość"')).mark_point()
    C = C.encode(Pt.Size('value:Q').legend(None))
    C = C.encode(Pt.Shape('plus:N').scale(range=['triangle-up', 'triangle-down']).legend(None))
    C = C.encode(Pt.Y('index:N').title(None).axis(None))
    C = C.encode(x=Pt.datum('wartość'))

    B = Pt.Chart(c).mark_text()
    B = B.encode(Pt.Text('value:Q', format='.2f'))
    B = B.encode(Pt.X('variable:N').title(None))
    B = B.encode(Pt.Y('index:N').title(None))

    P = P.properties(width=0.4*A4.W, height=0.4*A4.W)

    R = R.properties(width=0.05*A4.W, height=0.1*A4.H)
    Z = Z.properties(width=0.4*A4.W, height=0.1*A4.H)
    B = B.properties(width=0.2*A4.W, height=0.1*A4.H)
    C = C.properties(width=0.05*A4.W, height=0.1*A4.H)
    S = S.properties(width=0.05*A4.W, height=s.shape[0]*0.01*A4.H)

    return S | (P & (Z | (R + mR)).resolve_scale(y='shared') & (B | C))

  P = plot()
  k = Translate.get(model, model)
  P = P.properties(title=[f'{k}; grupowanie:', 'i: ' + ', '.join(I0), 'j: ' + ', '.join(IY)])

  return m, P

BDL = BDLwojload(path='GUS/BDL/data.csv', regions=gloc.region[1])

web = network(rprt.valid, endo.data, gloc.dist,
              spatial=['lat', 'lon'], temporal=['grant', 'application'],
              Jsim=[f'clsf-{l}' for l in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']],
              feats=['entity', 'pgid', 'wgid'], borders=gloc.region[0])

web = web.map(('cache/grph/edges.pkl', 'cache/grph/nodes.pkl', 'fig/grph/M.png'))

agg = transfer(web[0], web[1], BDL)


FLOW = dict(web=web, 
            linr=regr('linr', agg, 'transfer').map((None, 'fig/grph/linr.png')), 
            zip=regr('zip', agg, 'transfer').map((None, 'fig/grph/zip.png')), 
            pois=regr('pois', agg, 'transfer').map((None, 'fig/grph/pois.png')),
            nelo=regr('nelo', agg, 'transfer').map((None, 'fig/grph/nelo.png'))

          )