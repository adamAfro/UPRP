r"""
\newpage
\section{Dyfuzja wiedzy innowacyjnej między województwami}
"""

#lib
import lib.flow, grph, gloc

#calc
import pandas
from pandas import DataFrame as DF, Series as Se
from scipy.stats import pearsonr

#plot
import altair as Pt
from util import A4

@lib.flow.map(('fig/difw/F.pdf', 'tbl/difw/F.tex'))
@lib.flow.init(grph.network[0], grph.network[1], gloc.region[1])
def citekind(edges:DF, nodes:DF, regions:DF):

  r"""
  \begin{multicols}{2}
  \chart{fig/difw/F.pdf}
  { Wykres cytowań zawartych w patentach z uwzględnieniem
    rodzaju pochodzenia cytowanych i cytujących patentów }

  \columnbreak

  Wykresy obok przedstawiają zliczenie ilości
  osób cytujących i cytowanych w patentach z uwzględnieniem
  ich pochodzenia.
  Widać znaczący fakt, że rozkład jest nierównomierny między
  województwami. Małopolska i Mazowsze to główne
  ośrodki, w których zamieszkują osoby pełniące role patentowe.
  Wysoko plasuje się też woj. śląskie i dolnośląskie.
  Warto zauważyć że duża większość osób jest autorami
  patentów nie cytujących żadnego innego polskiego patentu.
  Podobnie w przypadku bycia patentem cytowanym --- patenty
  tych osób rzadko kiedy są cytowane zarówno wewnątrz
  województwa jak i na zewnątrz.
  Faktem jest jednak, że osoby cytujące korzystaja zarówno
  z patentów wewnątrz jak i na zewnątrz województwa.
  Podobnie w kwestii bycia cytowanym --- osoby te są
  cytowane zarówno przez osoby wewnątrz jak i na zewnątrz
  województwa najczęściej, jeśli się tak w ogóle dzieje.
  Warto zaznaczyć stosukowo jednorodny rozkład.
  Okresowe wahania w niektórych województwach są raczej 
  mało znaczące, dotyczą najczęściej tego czy patenty są cytowane
  i czy cytują, bardziej niż samego faktu powstawania patentów,
  które później otrzymują ochronę.

  \end{multicols}
  """

  N0 = nodes
  E0 = edges
  R0 = regions

 #zakres dyfuzji
  E0['internal'] = E0['wgid'] == E0['wgidY']
  EI = E0.query('  internal')
  EE = E0.query('~ internal')

 #wpływ patentów
  N0['cytowany wewn.'] = N0['doc'].isin(EI['from'])
  N0['cytowany zewn.'] = N0['doc'].isin(EE['from'])
  N0['generator'] = N0.apply(lambda x: ' i '.join([k for k in ['cytowany wewn.', 'cytowany zewn.'] if x[k]]), axis=1)
  N0['generator'] = N0['generator'].replace({'cytowany wewn. i cytowany zewn.': 'cytowany w.&z.'})
  N0['generator'] = N0['generator'].replace({'': 'nie cytowane'})

  N0['cytujący wewn.'] = N0['doc'].isin(EI['to'])
  N0['cytujący zewn.'] = N0['doc'].isin(EE['to'])
  N0['synthesis'] = N0.apply(lambda x: ' i '.join([k for k in ['cytujący wewn.', 'cytujący zewn.'] if x[k]]), axis=1)
  N0['synthesis'] = N0['synthesis'].replace({'cytujący wewn. i cytujący zewn.': 'cytujący w.&z.'})
  N0['synthesis'] = N0['synthesis'].replace({'': 'nie cytujące'})

 #ograniczenie danych
  E0 = E0[['wgid', 'year', 'wgidY', 'yearY', 'to', 'from', 'internal']].copy()
  N0 = N0[['wgid', 'year', 'doc', 'generator', 'synthesis']].copy()
  N0 = N0.query('year >= 2011')

 #regionalizacja
  R0 = R0[['gid', 'name']]
  E0 = E0.set_index('wgid').join(R0.set_index('gid')).reset_index()
  E0 = E0.set_index('wgidY').join(R0.set_index('gid').add_suffix('Y')).reset_index()
  N0 = N0.set_index('wgid').join(R0.set_index('gid')).reset_index()

 #grupowanie (...) regionalne
  gN = N0.groupby(['name', 'year', 'generator', 'synthesis'])
  N = gN.size().rename('size').reset_index()

 #wymiary
  x = Pt.X('year', type='ordinal')
  x = x.axis(values=[2011, 2020])
  y = Pt.Y('size', type='quantitative')
  y = y.axis(values=[0,1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000])
  f = Pt.Column('synthesis', type='nominal')
  c = Pt.Color('generator', type='nominal')
  c = c.legend(orient='bottom', columns=3)
  c = c.scale(range=['black', 'red', 'blue', 'grey'])
  n = Pt.Row('name', type='nominal')

 #etykiety
  N['name'] = N['name'].str[:4]+'.'
  for a in [x, y, f, c, n]: a.title=None 

  c = c.title('Pochodzenie')

 #wykres
  p0 = Pt.Chart(N.query('synthesis == "nie cytujące"')).mark_bar().encode(x, y, c, f, n)
  p0 = p0.properties(width=0.1*A4.W, height=0.04*A4.H)

  p = Pt.Chart(N.query('synthesis != "nie cytujące"')).mark_bar().encode(x, y, c, f, n)
  p = p.properties(width=0.1*A4.W, height=0.04*A4.H)

 #korelacja
  n = N.groupby(['name', 'year'])['size'].sum()

  X = N.pivot_table('size', ['name', 'year'], ['generator'], 'sum', fill_value=0)
  X = X.drop(columns='nie cytowane').sum(axis=1)

  Y = N.pivot_table('size', ['name', 'year'], ['synthesis'], 'sum', fill_value=0)
  Y = Y.drop(columns='nie cytujące').sum(axis=1)

  r = Se([X.corr(Y), X.corr(n), Y.corr(n)], index=['$v_x, v_y$', 
                                                   '$v_x, v$',
                                                   '$v_y, v$'])
  r.index.name = '$a, b$'
  r = r.rename('$corr(a, b)$ (Pearson)').to_frame()

  return p0 | p, r.to_latex()

@lib.flow.map('fig/difw/F-mx.pdf')
@lib.flow.init(grph.network[0], gloc.region[1])
def mx(edges:DF, regions:DF):

  r"""
  \subsection{Cytowania w raportach o stanie techniki pomiędzy województwami}

  \chart{fig/difw/F-mx.pdf}
  { Macierz cytowań w raportach o stanie techniki z uwzględnieniem lat. }

  Wykres powyżej przedstawia macierz cytowań w raportach o stanie techniki
  pomiędzy województwami. Przedstawia ilość osób cytujących
  w raportach o stanie techniki z uwzględnieniem lat i województw pochodzenia.
  Rząd wykresu wskazuje
  na województwo osoby cytowanej, 
  a kolumna, województwo osoby cytującej.
  Szczególny klaster cytowań widać wśród województw
  mazowieckiego, małopolskiego, śląskiego, dolnośląskiego, łódzkiego oraz wielkopolskiego.
  Po za tym zgrupowaniem, taka międzywojewódzka wymiana wiedzy jest rzadka.
  W samodzielnych relacjach widać zwiazek województwa śląskiego i podkarpackiego.
  Osoby z województwa śląskiego często cytują patenty osób z województwa podkarpackiego.
  Tutaj warto zaznaczyć duży dystans między tymi województwami.
  Czasowo, wymiana między województwami zachowuje relatywnie jednorodny charakter. 
  Występują częste wahania przedstawiające wzrost zainteresowania między województwami.
  Są to jednak przypadki rzadkie w stosunku do wszystkich relacji w całym okresie.
  """

 #dane
  E0 = edges
  R0 = regions

 #regionalizacja
  R0 = R0[['gid', 'name']]
  E0 = E0.set_index('wgid').join(R0.set_index('gid')).reset_index()
  E0 = E0.set_index('wgidY').join(R0.set_index('gid').add_suffix('Y')).reset_index()

 #grupowanie (...) regionalne
  gE = E0.groupby(['name', 'nameY', 'yearY'])

 #krawędzie regionalne
  E = gE.size().rename('size').to_frame()
  E = E.join(gE.agg({'distance': 'mean'}))
  E = E.reset_index()
  E.loc[E['name'] == E['nameY'], 'size'] = 0

 #segregacja na regiony
  K = list( set(E['nameY'].unique().tolist() + E['name'].unique().tolist()) )
  S = DF({ k0: [E.query(f'(name == "{k0}") & (nameY == "{k}")') for k in K] for k0 in K }, index=K)
  S = S.map(lambda X: X if not X.empty else None)

 #sortowanie
  o = S.map(lambda X: X['size'].sum() if X is not None else 0)
  o = o.sum().sort_values(ascending=False).index
  S = S.loc[o, o]

 #wymiary
  x = Pt.X('yearY', type='ordinal')
  y = Pt.Y('size', type='quantitative')
  y = y.axis(values=[0, 500], labels=False)
  c = Pt.Color('distance', type='quantitative')
  c = c.scale(range=['green', 'blue', 'red', 'black'], domain=(0, 500))

 #opisy wymiarów
  x = x.axis(None)
  y = y.title(None)
  c = c.legend(orient='bottom').title('Średnia odległość')

 #wykresy słupkowe
  F = S.map(lambda X: Pt.Chart(X).mark_bar().encode(x, y, c) if X is not None 
                 else Pt.Chart(X).mark_bar(fill=None))

 #teksty podpisujące województwa
  T = [Pt.Chart().mark_text().encode(text=Pt.datum(k[:4]+'.')) for k in S.keys()]
  for i, x in enumerate(F.index): F.iloc[i,i] = T[i]

 #dodanie etykiet osi do skrajnych
  y0 = Pt.Y('size', type='quantitative').title(None).axis(values=[0, 500])
  F.iloc[+1:, 0] = F.iloc[+1:, 0].apply(lambda p: p.encode(y0))
  x0 = Pt.X('yearY', type='ordinal').title(None).title(None).axis(values=[2011, 2022])
  F.iloc[-1,:-1] = F.iloc[-1,:-1].apply(lambda p: p.encode(x0))

 #łączenie
  F = F.map(lambda p: p.properties(width=0.05*A4.W, height=0.04*A4.W))
  p = Pt.concat(*[z for v in F.values.tolist() for z in v], columns=16, spacing=0)
  p = p.resolve_scale(x='shared', y='shared')

  return p