r"""
\section{Dyfuzja wiedzy innowacyjnej w powiatach}
"""

#lib
import lib.flow, grph, gloc

#calc
from pandas import DataFrame as DF, Series as Se
from geopandas import GeoDataFrame as GDF
from scipy.cluster.hierarchy import linkage, fcluster

#plot
import altair as Pt
from util import A4

@lib.flow.map('fig/difp/F.pdf')
@lib.flow.init(grph.network[0], grph.network[1], gloc.region[2])
def citekind(edges:DF, nodes:DF, regions:DF):

  r"""

  \begin{multicols}{2}

    \chart{fig/difp/F.pdf}
    { Patenty w Polsce 
        w zależności od pochodzenia cytowań
          w raportach o stanie techniki
            na poziomie powiatów }

      \columnsbreak

      Wykresy słupkowe po lewej
        zawierają informacje na temat
          pochodzenia cytowań w patentach
          oraz tego jak były cytowane.
      Zliczają ilość osób, 
        które tworzyło dany patent
          posługując się wiedzą
            o danej klasyfikacji.
      Patenty oznaczone jako 
        \textit{niecytowane} oraz
        \textit{niecytujące}
        odnoszą się do 3 przypadków:
          (1) raport o stanie techniki nie zawierał żadnych odniesień;
          (2) odniesienia patentowe w raporcie dotyczą zagranicznych patentów
              albo prac nie będących patentami;
          (3) nie udało się poprawnie zidentyfikować patentów z relacji.
      Przedstawione są 2 grupy: 
        powiaty miast szczególnie istotnych 
          w dyfuzji wiedzy
        oraz
        pozostałe powiaty.
      Spadek liczby 
        patentów cytowanych
          po roku 2015 należy tłumaczyć faktem,
            że są to patenty świeże,
              a ich cytowania dopiero nastąpią,
                albo nie są jeszcze w systemie.

      \end{multicols}

    Etykiety \textit{x-wewn}, \textit{x-zewn}; \textit{y-wewn}, \textit{y-zewn}
      oznaczają odpowiednio:
        osoby, których patenty są cytowane w innch patentach wewnętrznie / zewnętrznie;
        osoby cytujące inne patenty wewnętrznie / zewnętrznie.
    Zapisy \textit{x-w.&z}, \textit{y-w.&z}
      oznaczają cytowania zarówno wewnętrzne jak i zewnętrzne.
    \textit{Wnętrze} oraz \textit{zewnętrze}
      odnoszą się do lokacji osób powiązanych relacją
      --- tego czy są w tym samym powiecie (w.) czy nie (z.).

    Głównym wnioskiem z wykresu jest fakt, 
      że 7 powiatów było miejscem zamieszkania
        ponad połowy osób tworzących patenty.
    Ilość patentów w kolejnych latach jest niemal jednorodna,
      ale można przypuszczać o trendzie spadkowym.
    Wyjątkiem jest tu miasto Lublin.
      W przypadku powiatu miasta Lublin nie obserwujemy spadku,
        dodatkowo należy zaznaczyć, że ilość patentów,
          które korzystały z wiedzy z tego samego powiatu,
            a jednocześnie innych regionów Polski
            rośnie w czasie od 2016 roku.
    W przypadku powiatu Warszawa 
      mamy do czynienia z ogólnym spadkiem.
        Skupiając się jednak na patentach,
          które korzystały z wiedzy z innych regionów Polski,
            oraz samej Warszawy; obserwujemy 
              okresowy wzrost w latach 2014-2017.
  """

  N0 = nodes
  E0 = edges
  R0 = regions

 #zakres dyfuzji
  E0['internal'] = E0['pgid'] == E0['pgidY']
  EI = E0.query('  internal')
  EE = E0.query('~ internal')

 #wpływ patentów
  N0['x-wewn.'] = N0['doc'].isin(EI['from'])
  N0['x-zewn.'] = N0['doc'].isin(EE['from'])
  N0['generator'] = N0.apply(lambda x: ' i '.join([k for k in ['x-wewn.', 'x-zewn.'] if x[k]]), axis=1)
  N0['generator'] = N0['generator'].replace({'x-wewn. i x-zewn.': 'x-w.&z.'})
  N0['generator'] = N0['generator'].replace({'': 'nie cytowane'})

  N0['y-wewn.'] = N0['doc'].isin(EI['to'])
  N0['y-zewn.'] = N0['doc'].isin(EE['to'])
  N0['synthesis'] = N0.apply(lambda x: ' i '.join([k for k in ['y-wewn.', 'y-zewn.'] if x[k]]), axis=1)
  N0['synthesis'] = N0['synthesis'].replace({'y-wewn. i y-zewn.': 'y-w.&z.'})
  N0['synthesis'] = N0['synthesis'].replace({'': 'nie cytujące'})

 #ograniczenie danych
  E0 = E0[['pgid', 'year', 'pgidY', 'yearY', 'to', 'from', 'internal']].copy()
  N0 = N0[['pgid', 'year', 'doc', 'generator', 'synthesis']].copy()
  N0 = N0.query('year >= 2011')

 #regionalizacja
  R0 = R0[['gid', 'name']]
  E0 = E0.set_index('pgid').join(R0.set_index('gid')).reset_index()
  E0 = E0.set_index('pgidY').join(R0.set_index('gid').add_suffix('Y')).reset_index()
  N0 = N0.set_index('pgid').join(R0.set_index('gid')).reset_index()

 #grupowanie (...) ilościowe
  gN = N0.groupby(['pgid', 'name', 'year', 'generator', 'synthesis'])
  N = gN.size().rename('size').reset_index()

  s = N.groupby('pgid')['size'].sum().rename('sum').reset_index()
  s['cluster'] = fcluster(linkage(s[['sum']], method='ward', metric='euclidean'), 2, criterion='maxclust')
  N = N.set_index('pgid').join(s.set_index('pgid')['cluster']).reset_index()

 #zgrupowanie mniejszych
  U = N.query('cluster == 1').groupby(['year', 'generator', 'synthesis'])['size'].sum().reset_index()
  U['name'] = 'pozostałe'
 #zgrupowanie szczegółowe większych
  Z = N.query('cluster == 2').drop(columns=['cluster', 'pgid'])

 #wymiary
  x = Pt.X('year', type='ordinal')
  x = x.axis(values=[2011, 2020])
  y = Pt.Y('size', type='quantitative')
  y = y.axis(values=[0,1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000])
  f = Pt.Column('synthesis', type='nominal')
  c = Pt.Color('generator', type='nominal')
  c = c.legend(orient='bottom', columns=3)
  c = c.scale(range=['grey', 'red', 'blue', 'black'])
  n = Pt.Row('name', type='nominal')

 #etykiety
  Z['name'] = Z['name'].str.split(' ').apply(lambda x: x[1])
  for a in [x, y, f, c, n]: a.title=None 

  c = c.title('Pochodzenie')

 #wykres zgrupowania
  u0 = Pt.Chart(U.query('synthesis == "nie cytujące"')).mark_bar().encode(x, y, c, f, n)
  u0 = u0.properties(width=0.09*A4.W, height=0.04*A4.H)

  u = Pt.Chart(U.query('synthesis != "nie cytujące"')).mark_bar().encode(x, y, c, f, n)
  u = u.properties(width=0.09*A4.W, height=0.04*A4.H)

 #wykres szczegółowy
  p0 = Pt.Chart(Z.query('synthesis == "nie cytujące"')).mark_bar().encode(x, y, c, f, n)
  p0 = p0.properties(width=0.09*A4.W, height=0.04*A4.H)

  p = Pt.Chart(Z.query('synthesis != "nie cytujące"')).mark_bar().encode(x, y, c, f, n)
  p = p.properties(width=0.09*A4.W, height=0.04*A4.H)

  return (p0 | p) & (u0 | u)

@lib.flow.map('fig/difp/M-x.pdf')
@lib.flow.init(grph.network[0], gloc.region[2], gloc.region[0])
def xmap(edges:DF, regions:GDF, borders:GDF):

  r"""
  \chart{fig/difp/M-x.pdf}
  { Mapa powiatów 
      z zaznaczoną ilością osób, 
        które zostały cytowane }

  Powyższe mapy przedstawiają, 
    kolorystycznie, 
    ilość osób z danego powiatu, 
      które aplikowały o ochronę patentową,
    a ich patenty 
      zostały cytowane 
        przez inne, późniejsze patenty.
  Są to więc obszary 
    skupiające osoby 
      generujące wiedzę.
  Przedstawione są na nim lata 2011-2019.
  Rok 2020 został pominięty, z tej przyczyny,
    że właściwie już od 2017 widać znaczący spadek
      ogólnej liczby osób spełniających kryteria.

  Na mapie obserwujemy powiaty 
    o skrajnie niskiej, 
    niskiej 
    oraz 
    dużej ilości osób.
  Powiaty z dużą ilością osób nie sąsiadują ze sobą.
  Z kolei powiaty o niskiej ilości osób
    są nierównomiernie rozłożone na terenie kraju.
  Wyjątkiem jest Śląsk, który niemal ciągle zawierał,
  choć rozproszoną, duża populację osób będących przedmiotem badania.
  Pozostałe obszary sa powiatami o skrajnie niskiej ilości osób.
  Są to obserwacje dla całego przedstawionego okresu.
  W poszczególnych latach obserwujemy dynamiczne zmiany.
  W 2011 roku miasta Łódź i Wrocław zawierały najwięcej osób
  powiązanych z tworzeniem patentów, które później były cytowane.
  Rok później główną siedzibą dalej był Wrocław, a Łódź znacząco
  straciła na znaczeniu. Był to jednocześnie szczyt popularności
  Warszawy dla osób spełniających kryteria późniejszego cytowania.
  Podobnie Poznań wraz z okolicami oraz Kraków były znaczące.
  W 2013 roku, posiadające wcześniej także relatywnie silną pozycje
  Katowice, osiągneły szczyt, także Warszawa i Wrocław były wtedy znaczące.
  Kolejne lata zawierają przejścia w dominacji wcześniej wymienionych
  powiatów. Dla całego okresu należy także wyróżnić inne duże miasta
  wojewódzkie, tj.: Poznań, Szczecin i Lublin. W całym okresie,
  wartości dla nich były znaczące, choć w mniejszym stopniu niż
  te wymienionych wcześniej miast oraz z mniejszą dynamiką.
  Stopniowy spadek z roku na rok, od 2013 należy tłumaczyć
  faktem, że patenty mogą być cytowane w przyszłości, więc
  wraz z ich wiekiem, prawdopodobieństwo tego, że były
  cytowane naturalnie rośnie.
  """

  E0 = edges
  R = regions
  B = borders

 #ograniczenie danych
  E0 = E0.query('(year >= 2011) & (year < 2020)')
  R.geometry = R.geometry.simplify(0.02, preserve_topology=True)
  B.geometry = B.geometry.simplify(0.02, preserve_topology=True)

 #grupowanie krawędzi
  gE = E0.groupby(['year', 'pgid'])
  E = gE.size().rename('size').reset_index()
  E = E.set_index('pgid').join(R.set_index('gid')).reset_index()
  E = GDF(E, geometry='geometry')
  E = E.sort_values(['size'], ascending=False)
  E['large'] = E['size'] > 1000

 #segregowanie
  K = E['year'].unique()
  S = Se([E.query(f'year == {k}').drop(columns='year') for k in K], index=K)

 #wymiary
  C = ['white']+[x for c in ['grey', 'yellow', 'orange', 'red', 'violet', 'blue', 'darkblue'] for x in [c,c]]
  c = Pt.Color('size', type='quantitative')
  c = c.scale(range=C, domainMin=0)
  c = c.title('Ilość osób').legend(orient='bottom')

  xL = Pt.Latitude('lat', type='quantitative')
  yL = Pt.Longitude('lon', type='quantitative')
  tL = Pt.Text('name', type='nominal')

 #wykresy
  m0 = Pt.Chart(B).mark_geoshape(fill=None, stroke='black', strokeWidth=0.001*A4.W).project('mercator')
  M = S.map(lambda m: Pt.Chart(m).mark_geoshape().encode(c).project('mercator')).sort_index()
  M = M.map(lambda m: (m0 + m).properties(width=0.5*A4.W, height=0.5*A4.W))
  M = M.reset_index().apply(lambda m: m[0].properties(title=f'Rok {m["index"]}'), axis=1)
  m = Pt.concat(*M.values.tolist(), columns=3, spacing=0)

  return m

@lib.flow.map('fig/difp/M-y.pdf')
@lib.flow.init(grph.network[0], gloc.region[2], gloc.region[0])
def ymap(edges:DF, regions:GDF, borders:GDF):

  r"""
  \newpage
  \chart{fig/difp/M-y.pdf}
  { Mapa powiatów z 
      zaznaczoną ilością osób, 
        które cytowały inne patenty 
          z Polski }

  Powyższe mapy przedstawiają,  
    jak wcześniej --- kolorystycznie,
    ilość osób z danego powiatu,
      które cytowały patenty z Polski.
  Można więc stwierdzić, że są to obszary
  charakteryzyjące się potencjałem do syntezy wiedzy.

  Synteza wiedzy, podobnie jak jej generacja,
    charakteryzuje się skupiskami osób powiązanych z patentami
      o skrajnie niskim, niskim i dużym zagęszczeniu.
  Większość obszaru Polski to powiaty o skrajnie niskim zagęszczeniu.
  Występowanie powiatów o niskim zagęszczeniu jest mniej częste,
    a same powiaty są rozporszone po kraju, jednak tutaj
      także należy wskazać obszar Śląska jako szczególnie istotny.
  Powiaty o dużym zagęszczeniu osób, które cytowały inne patenty
    z Polski, nie sąsiadują ze sobą.
  Są to Warszawa, Wrocław, Kraków, Poznań, Katowice, Lublin, Szczecin.
  Wyjątkowe tu jest znaczenie miasta Dębica w woj. podkarpackim.
  W porównaniu z innymi ośrodkami jest to małe miasto,
  jednak osoby korzystajace z wiedzy z innych patentów
  są tu szczególnie liczne, a okoliczne powiaty także
  wykazują, choć niskie, to znaczące zagęszczenie.
  Zmienność w poszczególnych latach opiera się
  o roczne wahania w liczbie osób korzystających
  z wiedzy z innych patentów. W badanych latach, miasta
  o dużym zagęszczeniu osób utrzymują relatywnie wysokie zagęszczenie,
  przy czym zmiany w niej są duże.
  W Warszawie widać stopniowy wzrost od 2012 do 2015, a następnie
  stopniowy spadek, we Wrocławiu obserwujemy podobne zjawisko
  z rocznym przyśpieszeniem. W 2016 Wrocław zaczyna ponowny wzrost ze
  szczytem w 2018 roku i ponowym powrotem do spakdów.
  Katowice mają swój szczyt w 2016 roku --- pozostałe lata są różne.
  W Lublinie widoczny wzrost zaczął się dopiero w 2018 roku.
  Szczecin do 2018 roku utrzymywał stałą wartość, ale 2019 był spadkiem.
  """

  E0 = edges
  R = regions
  B = borders

 #ograniczenie danych
  E0 = E0.query('(yearY > 2011) & (yearY <= 2020)')
  R.geometry = R.geometry.simplify(0.02, preserve_topology=True)
  B.geometry = B.geometry.simplify(0.02, preserve_topology=True)

 #grupowanie krawędzi
  gE = E0.groupby(['yearY', 'pgidY'])
  E = gE.size().rename('size').reset_index()
  E = E.set_index('pgidY').join(R.set_index('gid')).reset_index()
  E = GDF(E, geometry='geometry')
  E = E.sort_values(['size'], ascending=False)

 #segregowanie
  K = E['yearY'].unique()
  S = Se([E.query(f'yearY == {k}').drop(columns='yearY') for k in K], index=K)

 #wymiary
  C = ['white']+[x for c in ['grey', 'yellow', 'orange', 'red', 'violet', 'blue', 'darkblue'] for x in [c,c]]
  c = Pt.Color('size', type='quantitative')
  c = c.scale(range=C, domainMin=0)
  c = c.title('Ilość osób').legend(orient='bottom')

 #wykresy
  m0 = Pt.Chart(B).mark_geoshape(fill=None, stroke='black', strokeWidth=0.001*A4.W).project('mercator')
  M = S.map(lambda m: Pt.Chart(m).mark_geoshape().encode(c).project('mercator')).sort_index()
  M = M.map(lambda m: (m0 + m).properties(width=0.5*A4.W, height=0.5*A4.W))
  M = M.reset_index().apply(lambda m: m[0].properties(title=f'Rok {m["index"]}'), axis=1)
  m = Pt.concat(*M.values.tolist(), columns=3, spacing=0)

  return m