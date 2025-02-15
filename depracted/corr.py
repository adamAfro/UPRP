#DEPRECATED

"""
\subsection{Autokorelacja przestrzenna}

Autokorelacja przestrzenna mówi o braku losowości w położeniu punktów:
jeśli autokorelacja jest statystycznie istotna to punkty są skupione w 
globalne klastry przestrzenne\cite{Se-Ar-Da-Wo-20}.
W kontekście analizy patentowej, globalność odnosi się do poziomu krajowego.

\D{spat-weight}{Wagi przestrzenne}
{ macierz liczbowych wag przestrzennych $W$,
  która określa siłę zależności przestrzenne między obiektami.
  Indeks rzędu macierzy $W$ odnosi się do obiektu przestrzennego,
  a indeks kolumny do obiektu co do którego występuje zależność.}

Zależność wag przestrzennych w poniżeszej analizie dotyczy 
konkretnie tego jak blisko siebie są obiekty przestrzenne ---
wynika wyłącznie z położenia i sąsiedztwa regionów.
Sąsiedztwo jest określane zgodnie z metodą \foreign{queen contiguity}
--- znaczy to tyle, że dwa regiony sąsiadują ze sobą
jeśli mają wspólną granicę lub punkt.

\D{lag}{Lag}{
  Macierz $Y_{l}$ jest iloczynem wag przestrzennych $W$ i wektora cechy $Y$
  dla każdej obserwacji przestrzennej. Lag jest miarą zależności przestrzennej
  w sąsiedztwie.
  \begin{math}
    Y_{l} = W\cdot Y
  \end{math}}

\D{Moran-I}{Statystyka I Morana}{
\begin{math}
  I = (n / \sum_i \sum_j w_{ij})\cdot (\sum_i \sum_j w_{ij} z_i z_j / \sum_i z_i^2)
\end{math}}


  \subsection
{Udział klasyfikacji \ac{IPC} w profilu punktu}\label{udział-klasyfikacji}

Udział klasyfikacji odnosi się do tego jak wiele osób dostawało 
ochronę patentową w danej sekcji ze wskazanego punktu, 
bądź współpracowało przy takim dokumencie z innymi osobami.
"""


#lib
import lib.flow, endo, gloc

#calc
import pandas, numpy, geopandas as gpd
import libpysal as sal, esda as sale

#plot
import altair as Pt
from util import A4

@lib.flow.Flow.From()
def Moran(X:pandas.DataFrame, R:gpd.GeoDataFrame, counted: list, by:str):

  X[counted] = X[counted].map(lambda x: 1 if x > 0 else 0)
  X = X[counted + [by]]
  X['total'] = 1 
  K = ['total']+counted

  X = X .set_index(by)\
        .join(R.set_index('gid'), how='right')\
        .groupby(by)\
        .agg(dict(geometry='first', **{ k: 'sum' for k in K }))\
        .pipe(gpd.GeoDataFrame, geometry='geometry')

  w = sal.weights.Queen.from_dataframe(X)
  w.transform = 'r'

  S = pandas.DataFrame(index=K)
  S['global'] = [sale.Moran(X[k], w, permutations=1000) for k in K]
  S['I'] = S['global'].map(lambda x: x.I)
  S['p'] = S['global'].map(lambda x: x.p_sim)
  S = S.drop(columns='global').reset_index()

  return S

pAC = Moran(endo.data, gloc.region[2], [f'clsf-{l}' for l in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']], 'pgid')

plots = dict()

plots['T-Moran'] = lib.flow.forward(pAC, lambda X:(

  lambda X:
    X .eval('z = p < 0.05').replace({True: 'p < 0.05', False: ''})\
      .pipe(Pt.Chart)\
      .properties(width=.03*A4.W, height=0.15*A4.H)\
      .mark_circle()\
      .encode(Pt.Y('index').title(None),
              Pt.Size('I').legend(None),
              Pt.Color('z').title(None)\
                .scale(range=['#0f0', '#f00'])) |\

    X .pipe(Pt.Chart, title='I')\
      .properties(width=.03*A4.W, height=0.15*A4.H)\
      .mark_text()\
      .encode(Pt.Y('index').axis(None),
              Pt.Text('I', format='.2f')) |\

    X .pipe(Pt.Chart, title='p')\
      .properties(width=.03*A4.W, height=0.15*A4.H)\
      .mark_text()\
      .encode(Pt.Y('index').axis(None),
              Pt.Text('p', format='.2f'))

)(X.replace(dict(total='Ogólnie', **{ f'clsf-{k}': k for k in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'] }))))

for k, F in plots.items():
  F.name = k
  F.map(f'fig/corr/{k}.png')