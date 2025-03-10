r"""
\section{Graf skierowany raportów o stanie techniki}

  \label{grph}

  Poniższa analiza
    przyjmuje, że 
      cytowania między patentami można
      traktować jako krawędzie grafu skieroawnego.
  Graf ten 
    tworzony jest 
      w 2 etapach:
  pierwszym jest 
    utworzenie 
      grafu $G_P$ 
        między dokumentami $P$,
        z krawędziami $R$;
  drugim etapem jest
    przetworzenie grafu $G_P$
      na graf $G$,
        gdzie wierzchołkami są osoby zbioru $O$.
  Zbiór $P$
    zawiera 
      wszystkie patenty,
        o których ochronę 
          złożono aplikację.
  Zbiór $R$ jest zbiorem
    cytatów 
      z raportów o stanie techniki.
  Relacja $\rho$ 
    zapisywana jako relacja między dwoma patentami $p_x\ \rho\ p_y$,
    czytana jako \textit{patent $p_x$ cytowany jest w patencie $p_y$},
    jest prawdziwa
      jeśli
        patent $p_y$
          zawiera w swoim raporcie
            wzmiankę o patencie $p_x$.

  \begin{equation}
    R = \{ (p_x, p_y)\in P \times P\ \vert\ p_x\ \rho\ p_y \}
    \end{equation}

  Kierunek tego grafu 
    jest zgodny z przepływem informacji, 
      co znaczy, że patent 
        którego dotyczy raport 
        jest węzłem końcowym,
        a wszystkie patenty wymienione w raporcie 
          są węzłami początkowymi.

  \newpage
  \begin{multicols}{2}
    Po prawej 
      stronie zaprezentowany jest 
        przykładowy zestaw danych
          z sekcji \ref{rprt}, 
      jako graf 
        o wierzchołkach 
          $V = { p_1, p_2, p_3 }$
        i krawędziach 
          $E = \{ (p_2, p_1), (p_3, p_1), (p_3, p_2) \}$.
    W raporcie 
      patentu $p_1$ 
        znajduje się odwołanie
          do patentu $p_2$ i $p_3$;
      dla $p_2$: $p_3$;
      $p_3$ 
        nie zawierał odniesienia
          do żadnego patentu,
            który znajdował by się w bazie.
    \columnbreak

    \begin{figure}[H]\centering\begin{tikzpicture}
      \draw[draw=black, thin, solid] (-1.50,1.50) ellipse (0.50 and -0.50);
      \node[black, anchor=south west] at (-2.06,1.25) {$p_1$};
      \draw[draw=black, thin, solid] (1.50,2.50) ellipse (0.50 and -0.50);
      \draw[draw=black, thin, solid] (0.50,-0.50) ellipse (0.50 and -0.50);
      \node[black, anchor=south west] at (0.94,2.25) {$p_2$};
      \node[black, anchor=south west] at (-0.06,-0.75) {$p_3$};
      \draw[draw=black, -latex, thin, solid] (-0.14,0.04) -- (-0.92,0.96);
      \draw[draw=black, -latex, thin, solid] (0.78,0.32) -- (1.14,1.67);
      \draw[draw=black, -latex, thin, solid] (0.49,2.33) -- (-0.62,1.90);
      \end{tikzpicture}
      \caption{Graf dla przykładowego zestawu raportów \cref{fig:raport-ex}}
      \label{fig:raport-ex-G}\end{figure}\end{multicols}

"""

#lib
import lib.flow, gloc, rprt, subj, util

#calc
import pandas, altair as Pt, networkx as nx
from pandas import DataFrame as DF, Series as Se

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
  \skip

  Każdy patent 
    ma przyporządkowany 
      pewien zbiór osób $O_{p_i} \subset O$.
  Tymi osobami są osoby pełniące
    jedną z 
      analizowanych 
      ról patentowych 
      (patrz: \cref{role-patentowe}).
  Pełnienie roli patentowej w patencie $p_i$
    jest relacją $p_i\ \alpha\ o_j$,
      gdzie 
        $p_i$ jest patentem,
        a $o_j$ osobą;
  należy odczytywać:
    \textit{patent $p_i$ jest związany z osobą $o_j$}.
  Zbiór $A$ jest zbiorem par patentów i osób,
    który zbiera wszystkie prawdziwe relacje $\alpha$:
  \begin{equation}
    A = \{ (p_i, o_j)\in P \times O\ \vert\ p_i\ \alpha\ o_j \}
    \end{equation}

  Graf $G$ między osobami $O$, jako wierzchołkami, 
    jest grafem 
      skierowanym,
      o krawędziach $E$:

  \begin{equation}
    E = \{ (o_i, o_j)\in O \times O\ \vert\ \exists p_x, p_y\in P\ \land\ (p_x, o_i), (p_y, o_j)\in A\ \land\ (p_i, p_j)\in R \}
    \end{equation}

  Krawędzie tego grafu świadczą o 
    przepływie wiedzy 
      między osobami.
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

@lib.flow.map(('fig/grph/F-delay.pdf'))
@lib.flow.init(network[0])
def delayplot(edges:DF): 

  r"""
  \newpage
  \subsection{Rozkład opóźnienia cytowań patentowych}

    \begin{multicols}{2}

      \chart{fig/grph/F-delay.pdf}{
        Rozkład gęstości opóźnienia 
          w kolejnych latach,
          z uwzględnieniem średniej odległości}

      \columnbreak

      Po lewej stronie został 
        przedstawiony rozkład 
          długości okresu między 
            składaniem aplikacji patentów
            będących w relacjach cytowania ($\rho$)
          dla każdej krawędzi grafu $G$,
            tj. po między osobami
              pełniącymi role patentowe
                we wspomnianych patentach.
      U góry przedstawiony jest
        wykres gęstości.
      Pod nim wykresy słupkowe,
        każdy przedstawiający 
          sytuację dla danego roku,
        z osią $x$ przedstawiającą
          opóźnienie w dniach
            zgrupowanych do okresów 365 dni.
      Kolorystycznie przedstawiono
        zakres odległości
          między osobami 
            w jaki wpada dany dystans.
      Można zaobserwować
        znaczące wzrost gęstosci
          dla wartości w przedziale
             od 500 do 1500 dni.
      Wykresy słupkowe 
        wykazują podobną dynamikę
          do roku 2016.
      Później widać 
        spłaszczenie rozkładu.
      Dominującą odległością
        jest wartość $0$.
        Po za nią,
      wartości rozkładają się
        bez wykazywania 
          szczególnego trendu,
            innego niż wcześniej wspomniana 
              tendencja do występowania
                we wskazanym przedziale.
      Ogólnie, 
        pierwszy okres roczny w opóźnieniu
        jest rzadki,
          jednak widać zauważalny wzrost
            w roku 2013.
      W latach 2016, 2017, 2018 obserwujemy
        mały ale widoczny 
          wzrost zainteresowania patentami starszymi.
      Mowa tu o wartościach 9-11 lat.
      \end{multicols}
  """

  E0 = edges

 #wybór danych
  E0['delay'] = (E0.applicationY - E0.application).dt.days
  E0['yearY'] = E0.applicationY.dt.year
  E = E0[['yearY', 'delay', 'distance']].copy()
  E['distbin'] = pandas.cut(E['distance'], bins=[0, 100, 200, 300, 500, 1000], 
                             include_lowest=False)
  C = ['{ 0 }'] + [c for c in E['distbin'].cat.categories]
  E['distbin'] = E['distbin'].cat.set_categories(C, ordered=True).fillna('{ 0 }')
  E = E.sort_values(['yearY','distance'])

 #wymiary
  y = Pt.Y('count(delay)').title(None)
  x = Pt.X('delay:Q').bin(step=365).title('Opóźnienie')
  c = Pt.Color('distbin:O').sort(['{ 0 }'])
  c = c.legend(orient='bottom', columns=3)
  c = c.title('Przedział odległości')
  f = Pt.Row('yearY:O').title(None)

 #wykres
  p = Pt.Chart(E).mark_bar().encode(x, y, c, f)
  p = p.properties(width=0.5*A4.W, height=0.05*A4.H)
  p

  xD = Pt.X('delay:Q').title('Opóźnienie')
  yD = Pt.Y('density:Q').title(None)
  D = Pt.Chart(E).mark_area().encode(xD, yD)
  D = D.properties(width=0.5*A4.W, height=0.05*A4.H)
  D = D.transform_density('delay', as_=['delay', 'density'])

  return D&p

@lib.flow.map(('fig/grph/F-dist.pdf'))
@lib.flow.init(network[0])
def distplot(edges:pandas.DataFrame):

  r"""
  \subsection{
    Rozkład odległości 
      między osobami 
        cytującymi, a cytowanymi}

    \chart{fig/grph/F-dist.pdf}
    { Wykresy rozkładu 
        odległości między osobami 
          cytującymi, a cytowanymi }

    Powyższe wykresy 
      przedstawiają rozkład odległości 
        między osobami 
          cytującymi, a cytowanymi. 
    Wykresy słupkowe prezentują
      histogram wartości niezerowych po prawej, 
      oraz 
      porównanie 
        wartości zerowych 
        i 
        niezerowych 
          po lewej.
    Obserwujemy 
      znaczącą frakcję 
        wartości zerowych, 
          które stanowią 
            ponad 1/3 wszystkich odległości.
    Należy z tego wnosić, 
      że znaczna część cytowań
        zachodzi między osobami 
          z tej samej lokalizacji.
    Histogram zawiera także 
      zaznaczone wartości statystyk pozycyjnych.
    Średnia jest 
      znacząco większa 
        od mediany
      co wskazuje na skośność rozkładu.
    Wartości w przedziale 260-280 
      są charakterystycznie częste.
    Wykres gęstości poniżej 
      wskazuje na dwie wyraźne 
        grupy odległości, 
          jedną wokół 10, 
          a drugą wokół 270.
    Należy także zaznaczyć 
      długi ogon rozkładu ---
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

  \newpage

  \subsection
  { Rozkład odległości 
      między osobami 
        w kolejnych latach }

    \begin{multicols}{2}

      \chart{fig/grph/F-dist-yearly.pdf}
      { Rozkład odległości 
          między osobami 
            cytującymi, a cytowanymi 
              w kolejnych latach }

      \columnbreak

      Obok przedstawiony jest 
        rozkład dystansu
          między osobami cytującymi i cytowanymi
      Wykres pierwszy prezentuje
        sumę liczności 
          relacji cytowania 
            w danym roku.
      Widać na nim 
        pewną wariancję w kolejnych latach.
      Wartości w latach 2011 i 2012 są znacząco mniejsze.
        Należy to tłumaczyć brakami w lokalizacji dla starszych
          patentów, które nie mogły zostać uwzględnione.
      Wykresy poniżej
        to wykresy gęstości
          dla kolejnych lat.
      Gęstość zgromadzona jest
        wokół wartości zerowych.
      Pozostałe dystanse 
        są równomiernie rozłożone
          w kolejnych latach
        sięgając 700 kilometrów
          w skrajnych przypadkach.
      Można przypuszczać o istnieniu pewnej
      lokalnej mody w przedziale od 200 do 300 kilometrów,
      dla wszystkich lat, jednak jest ona niewielka w porównaniu
      z licznością dystansów bliskich zeru i zerowych.
      Większość 
        mieście się 
          w przedziale do 300 kilometrów.
      Zmiany z roku na rok są słabo zauważalne,
      co może świadczyć o stałej zależności między
      prawdopodobieństwem przepływu wiedzy, a dystansem.


      \end{multicols}
  """

  E = edges[[ 'yearY', 'distance' ]]

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

@lib.flow.map(('fig/grph/M-dist.pdf'))
@lib.flow.init(network[0], borders=gloc.region[1])
def distcart(edges, borders, spatial=['lat', 'lon']):

  r"""
  \newpage

  \subsection{
    Charakterystyka lokalizacji 
      na podstawie ilości 
        osób cytujących i 
        cytowanych}

    Ze względu na naturę rozprzestrzeniania się wiedzy 
      ---
      sieć
        jest tym słabsza 
          im bardziej jest rozproszona w przestrzeni 
        \cite{Kl-14} 
      ---
      jej głównym aspektem jest
        położenie osób,
          które jej ulegają.
    Rozróżniając miejsca ze względu
      na liczność osób
        biorących udział w procesie,
      możemy wskazać 
        lokalizacje mniej lub bardziej
          sprzyjające dla omawianego zjawiska.
    Fakt, że sieć przepływu wiedzy
      jest grafem skierowanym,
        pozwala na wskazanie
          punktów generujących wiedzę
          oraz tych, gdzie ulega syntezie
            na wewnętrzny użytek
              ich odbiorców.

    \begin{multicols}{2}

      \chart{fig/grph/M-dist.pdf}
        { Mapy z lokalizacjami 
            osób
              cytujących 
              i cytowanych }

        \columnbreak

      Na mapach obok 
        zaprezentowane są 
          lokalizacje 
            osób 
              cytujących oraz 
              cytowanych. 
        Kolor punktów 
          odpowiada 
            średniej 
              odległości między osobą, a osobą referującą 
            (żółty --- bliskie, czerwony --- dalekie).
      Rozmiar punktów 
        odpowiada 
          ilości referowanych osób:
        na wykresie górnym dotyczy 
          osób cytujących, 
        a na dolnym 
          --- 
          cytowanych.
      Ostatnia mapa 
        przedstawia
          różnicę 
            między ilością 
              osób cytujących i cytowanych 
                w danej lokalizacji.
            Rozmiar punktów jest tożsamy
              z wartością 
                tej różnicy.
        Kolor punktów odpowiada 
          dominującemu kierunkowi przepływu wiedzy, 
            gdzie 
              \textit{odpływ} oraz 
              \textit{dopływ}
                rozumiemy jako, odpowiednio, 
                  przewagę 
                    osób cytujących 
                    albo cytowanych.

      \end{multicols}

    Główną obserwacją jest 
      dysproporcja ilościowa między
        północą, 
        a resztą kraju.
      To na południu 
        znajdują się
          punkty o największej
            liczbie osób 
              cytujących i cytowanych.
      Punkty na północy są
        zarówno mniej liczne
          jak i o mniejszym nominale.
    Rozróżnienie  na generatory wiedzy i odbiorców,
      z uwzględnieniem samej skali przepływu,
      nie pozwala na jednoznaczne wskazanie
        na obszary o dominującej roli
          generatorów wiedzy, lub odbiorców.
      Po za wyjątkami 
        o niewielkiej skali,
          jedynym wyjątkiem dużej (>5000 relacji) skali 
            jest Dębica, 
              obserwujemy stosunek
              $\tfrac{\text{różnica}}{\text{cytujące} + \text{cytowane}}$ równy $0.4$,
              klasyfikujące to miasto jako 
                specyficzny punkt syntezy wiedzy.
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
  I = set(X.index) | set(Y.index)
  D = (Y.reindex(I, fill_value=0) - X.reindex(I, fill_value=0))[['id']].reset_index()
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

 #tylko do eksploracji
  x = X.drop(columns=['distance'])['id'].rename('generator')
  y = Y.drop(columns=['distance'])['id'].rename('synthesis')
  F = D.set_index(['lat', 'lon'])['id'].rename('diff').to_frame().join(x).join(y).fillna(0)
  F['transfer'] = F['generator']+F['synthesis']
  F['frac'] = F['diff'] / F['transfer']
  F.query('transfer > 1000').sort_values('frac', ascending=False)

  return (mX & mY).resolve_scale(color='shared') & mD