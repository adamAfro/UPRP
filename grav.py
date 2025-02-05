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

\end{itemize}

Ostatecznie otrzymujemy postać liniową dla zmiennej $T$ uwzględniającej
dodatkowe efekty:

\begin{equation} \ln T_{i,j} = \ln A
  + \alpha \ln N_i
  + \beta \ln N_j 
  + \gamma \ln J_{i,j}
  + \delta \ln {P}_{i,j}
  + \theta \ln D_{i,j}
  \end{equation}
"""



#lib
import lib.flow, grph

#calc
import pandas, numpy, statsmodels.api as sm

#plot
import altair as Pt
from util import A4

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

 #klasyfikacja per patent
  for l in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']:
    E[f'Npat{l}'] = (E[f'clsf-{l}'] > 0).astype(int)

  N = V.value_counts('pgid')
  G = E.groupby(['pgid', 'pgidY'])
  T = G.size()
  P = G['Adelay'].mean().rename('P')
  D = G['distance'].first().rename('D')

  Y = T.rename('T').reset_index()

  Y = Y.set_index(['pgid']).join(N.rename('i')).reset_index()

  N.index.name = 'pgidY'
  Y = Y.set_index(['pgidY']).join(N.rename('j'))

  Y = Y .reset_index().set_index(['pgid', 'pgidY'])\
        .join(D).join(P).reset_index()

  Y = Y.astype(float)
  Y['D'] = Y['D'].replace(0., 1.)

  return Y

@lib.flow.make()
def linr(preped:pandas.DataFrame):

  r"""
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

  m = sm.OLS(X['T'], sm.add_constant(X[['i', 'j', 'D', 'P']])).fit()

  X['Y'] = m.predict(sm.add_constant(X[['i', 'j', 'D', 'P']]))
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
def pois(preped:pandas.DataFrame):

  "Poission regression"

  X = preped
  X = numpy.log(X)

  m = sm.Poisson(X['T'], sm.add_constant(X[['i', 'j', 'D', 'P']])).fit()

  X['Y'] = m.predict(sm.add_constant(X[['i', 'j', 'D', 'P']]))
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

  S = pandas.DataFrame([('stat', 0)], columns=['label', 'value'])

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
def nelo(preped:pandas.DataFrame):

  "Negative Binomial regression with a log link"

  X = preped
  X = numpy.log(X)

  m = sm.GLM( X['T'], sm.add_constant(X[['i', 'j', 'D', 'P']]), 
              family=sm.families.NegativeBinomial(link=sm.families.links.log())).fit()


  X['Y'] = m.predict(sm.add_constant(X[['i', 'j', 'D', 'P']]))
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

  S = pandas.DataFrame([('stat', 0)], columns=['label', 'value'])

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



data = prep(grph.nodes0, grph.edges)

plots = dict()

plots['F-data'] = lib.flow.forward(data, lambda X, K=['T', 'i', 'j', 'D', 'P']: 

  Pt.vconcat(*[X[[k]].pipe(Pt.Chart).mark_bar()\
    .properties(width=0.7*A4.W, height=0.1*A4.H)\
    .encode(Pt.X(k).bin().title(k),
            Pt.Y(f'count({k})')\
              .scale(type='log')\
              .title(None) ) for k in K]) )


plots['F-linr'] = linr(data)
plots['F-pois'] = pois(data)
plots['F-nelo'] = nelo(data)

for k, F in plots.items():
  F.name = k
  F.map(f'fig/grav/{k}.png')