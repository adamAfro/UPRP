r"""
\subsection{Model grawitacyjny}

Model grawitacyjny inspirowany modelem fizycznym Newtona
jest stosowany do wyjaśniania wymiany handlowej między dwoma
regionami położonymi\cite{Go-Es-13}.
W kontekście patentów, modelowany jest przepływ wiedzy.

\begin{equation}T_{i,j} = A \cdot (N_i \cdot N_j) / D_{i,j}^\theta \end{equation}

Składniki równania reprezetnują następujące wartości:\begin{itemize}

\item \( T_{i,j} \) --- wykorzystanie patentów z regionu \( i \) 
                        w regionie \( j \)

\item \( A \) --- stała proporcjonalności
\item \( N_i \) --- ilość patentów \( i \)
\item \( N_j \) --- ilość patentów \( j \)
\item \( D_{i,j} \) --- odległość między \( i \) a \( j \)
\item \( \theta \) --- parametr odległości
\end{itemize}

Postać liniowa dla jest następująca:

\begin{equation} \ln X_{i,j} = \ln A 
  + \alpha \ln N_i 
  + \beta \ln N_j 
  + \theta \log D_{i,j} 
  \end{equation}

W modelu uwzględniamy także inne efekty: \begin{itemize}

\item $J_{i,j}$ --- podobieństwo Jaccarda dla klasyfikacji

\item ${P}_{i,j}$ --- średnia okresu po między składaniem aplikacji patentowych

\item $\stackrel{\sigma}{P}_{i,j}$ --- rozrzut okresu po między składaniem aplikacji patentowych

\end{itemize}

Ostatecznie otrzymujemy postać liniową dla zmiennej $T$ uwzględniającej
dodatkowe efekty:

\begin{equation} \ln T_{i,j} = \ln A
  + \alpha \ln N_i
  + \beta \ln N_j 
  + \gamma \ln J_{i,j}
  + \delta \ln {P}_{i,j}
  + \eta \ln \stackrel{\sigma}{P}_{i,j}
  + \theta \ln D_{i,j}
  \end{equation}
"""



#lib
import lib.flow, grph

#calc
import pandas, numpy, statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor as vif


#plot
import altair as Pt
from util import A4

Translate = { 'i': 'wejś.',
              'j': 'wyjś.',
              'D': 'dystans',
              'P': 'opóźnienie (śr.)',
              'Pd': 'opóźnienia (rt.)',
              'T': 'przepływ',
              'pearcorr': 'kor. Pearsona',
              'VIFC': 'VIF ze stałą',
              'J': 'Jaccard',
                                            }

class Jaccard:

  def m(a, b): return sum(map(min, a, b))
  def M(a, b): return sum(map(max, a, b))
  def j(a, b): return 1 if Jaccard.M(a, b) == 0 else Jaccard.m(a, b) / Jaccard.M(a, b)

@lib.flow.make()
def prep(nodes:pandas.DataFrame, edges:pandas.DataFrame):

  r"""
  Przygotowanie danych odbywa się przez agregację informacji
  o połączeniach między regionami na poziomie powiatowym.

  \begin{uwaga}
  Dystans zerowy, tj. dla obserwacji z jednego punktu,
  jest zamieniany z 0 na 1 dla uniknięcia błędów.
  Oznacza to, że zakładami minimalną karę na
  poziomie 1 kilometra
  \end{uwaga}

  \chart{../fig/grav/F-data.png}{Rozkład zmiennych}{}
  """

  E = edges
  V = nodes

  IPC = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']

 #klasyfikacja per patent
  for l in IPC:
    E[f'Npat{l}'] = (E[f'clsf-{l}'] > 0).astype(int)
    E[f'Npat{l}Y'] = (E[f'clsf-{l}Y'] > 0).astype(int)

  N = V.value_counts('pgid')
  G = E.groupby(['pgid', 'pgidY'])
  J = G.agg({**{ f'Npat{l}': 'sum' for l in IPC}, **{ f'Npat{l}Y': 'sum' for l in IPC}})
  J = J.apply(lambda x, left, right: Jaccard.j(x[left].values, x[right].values), axis=1,
              left=[f'Npat{l}' for l in IPC],
              right=[f'Npat{l}Y' for l in IPC]).rename('J')
  J = J + 1
  T = G.size()
  P = G['Adelay'].mean().rename('P')
  Pd = (G['Adelay'].std().fillna(1).replace({0: 1})/P).rename('Pd')
  D = G['distance'].first().rename('D')

  Y = T.rename('T').reset_index()

  Y = Y.set_index(['pgid']).join(N.rename('i')).reset_index()

  N.index.name = 'pgidY'
  Y = Y.set_index(['pgidY']).join(N.rename('j'))

  Y = Y .reset_index().set_index(['pgid', 'pgidY'])\
        .join(D).join(J).join(P).join(Pd).reset_index()

  Y = Y.astype(float)
  Y['D'] = Y['D'].replace(0., 1.)

  return Y

@lib.flow.make()
def linr(preped:pandas.DataFrame, K:list[str]):

  r"""
  \newpage
  \subsubsection{Wykres modelu grawitacyjnego}

  Wykres przedstawia porównanie wartości obserwowanych z wartościami
  predykowanymi przez model. Poniżej wykresu znajdują się statystyki
  modelu. Po prawej stronie znajdują się wartości współczynników wraz
  z ich statystykami. Dla porównania wartości współczynników zaznaczono
  ich wielkość na wykresie obok, okręgami. \textit{Wejś.} 
  oznacza ogólną ilość patentów w regionie źródłowym, a \textit{wyjś.} 
  w regionie docelowym. \textit{Dystans} to odległość między regionami.

  \chartside{../fig/grav/F-linr.png}{Wykres modelu grawitacyjnego}{
  Na wykresie można zaobserwować, dużo większy rozrzut w granicach
  0-4 predykowanych wartości. Tam też znajduje się większość obserwacji.
  \TODO{dalszy opis}
  }
  """

  X = preped
  X = numpy.log(X)

  m = sm.OLS(X['T'], sm.add_constant(X[K])).fit()

  X['Y'] = m.predict(sm.add_constant(X[K]))
  X['e'] = X['T'] - X['Y']

  R =X.pipe(Pt.Chart).mark_bar()\
      .properties(width=0.4*A4.W, height=0.05*A4.W)\
      .encode(Pt.X('e:Q').title('Reszty').bin(step=0.25),
              Pt.Y('count(e):Q').title(None))

  P =  X.pipe(Pt.Chart).mark_point(opacity=0.1)\
        .properties(width=0.4*A4.W, height=0.4*A4.W)\
        .encode(Pt.X('T')\
                  .scale(domain=[-1, 10])\
                  .title('Log. obserwacji'), 
                Pt.Y('Y')\
                  .scale(domain=[-1, 10])\
                  .title('Predykcje'))

  S = pandas.DataFrame([('R²', m.rsquared),
                        ('st.s.m.', m.df_model),
                        ('st.s.r.', m.df_resid),
                        ('AIC', m.aic),
                        ('BIC', m.bic),
                        ('F', m.fvalue),
                        ('p(F)', m.f_pvalue)], columns=['label', 'value'])

  S = S .pipe(Pt.Chart).mark_text()\
        .properties(width=0.05*A4.W, height=0.1*A4.H)\
        .encode(Pt.Text('value:Q', format='.1f'),
                Pt.Y('label:N').title(None))


  C = m .summary2().tables[1].reset_index()\
        .drop(columns=['[0.025', '0.975]'])\
        .melt(id_vars=['index'])
  C['value'] = C['value'].astype(float)
  C['index'] = C['index'].replace({'j': 'wyjś.',
                                   'i': 'wejś.',
                                   'D': 'dystans',
                                   'P': 'opóźnienie',
                                   'Pd': 'rozrzut opóźnienia',
                                   'const': 'stała'})
  C['variable'] = C['variable'].replace({'P>|t|': 'p-wartość',
                                         'Coef.': 'wartość',
                                          'Std.Err.': 'błąd std.',
                                          't': 't-wartość'})

  A = C.query('variable == "wartość"')\
      .pipe(Pt.Chart).mark_circle()\
      .properties(width=0.05*A4.W, height=0.1*A4.H)\
      .encode(Pt.Size('value:Q').legend(None),
              Pt.Y('index:N').title(None).axis(None),
              x=Pt.datum('wartość'))

  B = C .pipe(Pt.Chart).mark_text()\
        .properties(width=0.2*A4.W, height=0.1*A4.H)\
        .encode(Pt.Text('value:Q', format='.3f'),
                Pt.X('variable:N').title(None),
                Pt.Y('index:N').title(None))

  return P & R & (S | B | A)

@lib.flow.make()
def pois(preped:pandas.DataFrame, K:list[str]):

  r"""
  \newpage
  \chartside{../fig/grav/F-pois.png}{Model regresji Poissona}{}
  """

  X = preped
  X = numpy.log(X)

  m = sm.Poisson(X['T'], sm.add_constant(X[K])).fit()

  X['Y'] = m.predict(sm.add_constant(X[K]))
  X['e'] = X['T'] - X['Y']

  R =X.pipe(Pt.Chart).mark_bar()\
      .properties(width=0.4*A4.W, height=0.05*A4.W)\
      .encode(Pt.X('e:Q').title('Reszty').bin(step=0.25),
              Pt.Y('count(e):Q').title(None))

  P =  X.pipe(Pt.Chart).mark_point(opacity=0.1)\
        .properties(width=0.4*A4.W, height=0.4*A4.W)\
        .encode(Pt.X('T')\
                  .scale(domain=[-1, 10])\
                  .title('Log. obserwacji'), 
                Pt.Y('Y')\
                  .scale(domain=[-1, 10])\
                  .title('Predykcje'))
  S = pandas.DataFrame([('AIC', m.aic),
                        ('BIC', m.bic),
                        ('st.s.m.', m.df_model),
                        ('st.s.r.', m.df_resid)], columns=['label', 'value'])

  S = S .pipe(Pt.Chart).mark_text()\
        .properties(width=0.05*A4.W, height=0.1*A4.H)\
        .encode(Pt.Text('value:Q', format='.1f'),
                Pt.Y('label:N').title(None))

  C = m .summary2().tables[1].reset_index()\
        .drop(columns=['[0.025', '0.975]'])\
        .melt(id_vars=['index'])
  C['value'] = C['value'].astype(float)
  C['index'] = C['index'].replace({'j': 'wyjś.',
                                   'i': 'wejś.',
                                   'D': 'dystans',
                                   'P': 'opóźnienie',
                                   'const': 'stała'})
  C['variable'] = C['variable'].replace({'P>|t|': 'p-wartość',
                                         'Coef.': 'wartość',
                                          'Std.Err.': 'błąd std.',
                                          't': 't-wartość'})

  A = C.query('variable == "wartość"')\
      .pipe(Pt.Chart).mark_circle()\
      .properties(width=0.05*A4.W, height=0.1*A4.H)\
      .encode(Pt.Size('value:Q').legend(None),
              Pt.Y('index:N').title(None).axis(None),
              x=Pt.datum('wartość'))

  B = C .pipe(Pt.Chart).mark_text()\
        .properties(width=0.2*A4.W, height=0.1*A4.H)\
        .encode(Pt.Text('value:Q', format='.3f'),
                Pt.X('variable:N').title(None),
                Pt.Y('index:N').title(None))

  return P & R & (S | B | A)

@lib.flow.make()
def nelo(preped:pandas.DataFrame, K:list[str]):

  r"""
  \newpage
  \chartside{../fig/grav/F-nelo.png}{
  Model Generalized Linear Model (GLM) 
  z rodziny dystrybucji Negative Binomial 
  (ujemna dwumianowa) i funkcją linku logarytmicznego}{}
  """

  X = preped
  X = numpy.log(X)

  m = sm.GLM( X['T'], sm.add_constant(X[K]), 
              family=sm.families.NegativeBinomial(link=sm.families.links.log())).fit()


  X['Y'] = m.predict(sm.add_constant(X[K]))
  X['e'] = X['T'] - X['Y']

  R =X.pipe(Pt.Chart).mark_bar()\
      .properties(width=0.4*A4.W, height=0.05*A4.W)\
      .encode(Pt.X('e:Q').title('Reszty').bin(step=0.25),
              Pt.Y('count(e):Q').title(None))

  P =  X.pipe(Pt.Chart).mark_point(opacity=0.1)\
        .properties(width=0.4*A4.W, height=0.4*A4.W)\
        .encode(Pt.X('T')\
                  .scale(domain=[-1, 15])\
                  .title('Log. obserwacji'), 
                Pt.Y('Y')\
                  .scale(domain=[-1, 15])\
                  .title('Predykcje'))

  S = pandas.DataFrame([('AIC', m.aic),
                        ('BIC', m.bic),
                        ('st.s.m.', m.df_model),
                        ('st.s.r.', m.df_resid),
                        ('pseudo-R²', m.pseudo_rsquared()),
                        ('odchyl.', m.deviance),
                        ('chi.', m.pearson_chi2)], columns=['label', 'value'])

  S = S .pipe(Pt.Chart).mark_text()\
        .properties(width=0.05*A4.W, height=0.1*A4.H)\
        .encode(Pt.Text('value:Q', format='.1f'),
                Pt.Y('label:N').title(None))


  C = m .summary2().tables[1].reset_index()\
        .drop(columns=['[0.025', '0.975]'])\
        .melt(id_vars=['index'])
  C['value'] = C['value'].astype(float)
  C['index'] = C['index'].replace({'j': 'wyjś.',
                                   'i': 'wejś.',
                                   'D': 'dystans',
                                   'P': 'opóźnienie',
                                   'const': 'stała'})
  C['variable'] = C['variable'].replace({'P>|t|': 'p-wartość',
                                         'Coef.': 'wartość',
                                          'Std.Err.': 'błąd std.',
                                          't': 't-wartość'})

  A = C.query('variable == "wartość"')\
      .pipe(Pt.Chart).mark_circle()\
      .properties(width=0.05*A4.W, height=0.1*A4.H)\
      .encode(Pt.Size('value:Q').legend(None),
              Pt.Y('index:N').title(None).axis(None),
              x=Pt.datum('wartość'))

  B = C .pipe(Pt.Chart).mark_text()\
        .properties(width=0.2*A4.W, height=0.1*A4.H)\
        .encode(Pt.Text('value:Q', format='.3f'),
                Pt.X('variable:N').title(None),
                Pt.Y('index:N').title(None))

  return P & R & (S | B | A)

@lib.flow.make()
def varvis(X:pandas.DataFrame, K:list[str], k0:str):

  r"""
  \subsubsection{Wizualizalizacja zmiennych}
  """

 #hist
  P = { k: Pt.Chart(X[[k]]).mark_bar() for k in K}
  P = { k: p.encode(Pt.X(f'{k}:Q').bin().title(Translate.get(k, k)),
                    Pt.Y(f'count({k})').scale(type='log')\
                      .title(None)) for k, p in P.items() }
  E = { k0:p for k, p in P.items() if k == k0 }
  G = { k: p for k, p in P.items() if k != k0 }
  c = X[K].corr(method='pearson')

 #stat
  S = { k: pandas.DataFrame(index=[k]) for k in G.keys() }
  S = { k: s.assign(VIF=[vif(X[K].values, i)]) for i, (k, s) in enumerate(S.items()) }
  S = { k: s.assign(VIFC=[vif(X[K].assign(const=1).values, i)]) for i, (k, s) in enumerate(S.items()) }
  S = { k: s.assign(pearcorr=[c.loc[k, k0]]) for k, s in S.items() }
  S = { k: s.T.reset_index() for k, s in S.items() }
  S = { k: s.map(lambda x: Translate.get(x, x)) for k, s in S.items() }

  S = {k: Pt.Chart(s).mark_text() for k, s in S.items() }
  S = {k: p.encode( Pt.Text(k, format='.2f'),
                    Pt.Y('index:N').title(None)) for k, p in S.items()}

 #correxog
  c = c.unstack().rename('value').reset_index()
  c = c.map(lambda x: Translate.get(x, x))
  C = {k0: Pt.Chart(c).mark_rect() }
  C = { k: p.encode(Pt.X('level_0:N').title(None),
                    Pt.Y('level_1:N').title(None),
                    Pt.Color('value:Q').title(Translate.get('pearcorr', 'r'))\
                      .legend(orient='top')
                      .scale(scheme='blueorange', domain=[-1, 1])) for k, p in C.items() }

  Ct = {k0: Pt.Chart(c).mark_text(fontSize=8) }
  Ct = {k: p.encode(Pt.X('level_0:N').title(None),
                    Pt.Y('level_1:N').title(None),
                    Pt.Text('value:Q', format='.2f')) for k, p in Ct.items() }

 #rozrzut
  R0 = {k: Pt.Chart(X[[k, k0]]).mark_point(opacity=0.1).properties(width=0.1*A4.H, height=0.1*A4.H)  for k in P.keys() if k != k0 }
  R0 = {k: p.encode(Pt.X(k).title(None)\
                      .scale(),
                    Pt.Y(k0).title(Translate.get(k0, k0))\
                      .scale(type='log')) for k, p in R0.items() }

 #regresja
  R = {k: p.transform_regression(k, k0).mark_line(color='red') for k, p in R0.items() }
  R = {k: p.encode( Pt.X(k).scale(domain=[X[k].min(), X[k].max()]),
                    Pt.Y(k0).scale(type='log', domain=[X[k0].min(), X[k0].max()])) for k, p in R.items()}

  G = { k: p.properties(width=0.6*A4.W, height=0.1*A4.H) for k, p in G.items() }
  S = { k: p.properties(width=0.05*A4.W, height=0.1*A4.H) for k, p in S.items() }
  R = { k: p.properties(width=0.1*A4.H, height=0.1*A4.H) for k, p in R.items() }
  R0 = {k: p.properties(width=0.1*A4.H, height=0.1*A4.H) for k, p in R0.items()}
  G = { k: G[k]|S[k]|(R0[k]+R[k]) for k in G.keys() }

  E = { k: p.properties(width=0.6*A4.W, height=0.1*A4.H) for k, p in E.items() }
  C = { k: p.properties(width=0.1*A4.H, height=0.1*A4.H) for k, p in C.items() }
  E = { k: E[k]|(C[k]+Ct[k]) for k in E.keys() }

  return Pt.vconcat(*{ **E, **G }.values())


data = prep(grph.nodes0, grph.edges)

plots = dict()

plots['F-data'] = varvis(data, ['T', 'i', 'j', 'D', 'P', 'Pd', 'J'], 'T')


plots['F-linr'] = linr(data, ['i', 'j', 'D', 'P', 'Pd', 'J'])
plots['F-pois'] = pois(data, ['i', 'j', 'D', 'P', 'Pd', 'J'])
plots['F-nelo'] = nelo(data, ['i', 'j', 'D', 'P', 'Pd', 'J'])

for k, F in plots.items():
  F.name = k
  F.map(f'fig/grav/{k}.png')