r"""



    \section{Pozyskiwanie danych}
  \subsection{Pobieranie}\begin{multicols}{2}
Identyfikacja patentów w wewnętrznym systemie \ac{URPR} to przyporządkowanie
każdego patentu do 6-cyfrowego numeru, który jest unikalny dla
każdego zgłoszenia. Pobieranie danych dotyczących wszystkich patentów 
można więc przeprowadzić wysyłając zapytanie do interfejsu o każdy
patent po kolei. Zaczynając od \textit{000-001} i kończąc na \textit{999-999}.
W praktyce skuteczność kierowania zapytań kończy się na \textit{450-000}.
W odpowiedzi otrzymuje się dane w formacie \ac{XML}.

\columnbreak
\begin{figure}[H]
\footnotesize
\centering
\begin{tikzpicture}
	\draw[draw=black, thin, solid] (-3.00,3.00) rectangle (-1.00,2.00);
	\draw[draw=black, thin, solid] (0.00,3.00) rectangle (2.00,2.00);
	\node[black, anchor=south west] at (-3.06,2.25) {skrypt};
	\node[black, anchor=south west] at (-0.06,2.25) {\ac{API}};
	\draw[draw=black, thin, solid] (-2.00,2.00) -- (-2.00,-3.00);
	\draw[draw=black, thin, solid] (1.00,2.00) -- (1.00,-3.00);
	\node[black, anchor=south west] at (-2.06,0.25) {kod 6-cyfrowy};
	\draw[draw=black, -latex, thin, solid] (-2.00,0.00) -- (1.00,0.00);
	\node[black, anchor=south west] at (-1.06,-1.75) {plik XML};
	\draw[draw=black, -latex, thin, solid] (1.00,-2.00) -- (-2.00,-2.00);
	\draw[draw=black, thin, solid] (-3.00,-3.00) rectangle (-1.00,-3.50);
	\draw[draw=black, thin, solid] (0.00,-3.00) rectangle (2.00,-3.50);
\end{tikzpicture}
\caption{Schemat pobierania danych z \ac{API} \ac{UPRP}}
\end{figure}
\end{multicols}



  \subsection{Wstępna struktura}\begin{multicols}{2}

Struktura pobranych plików \ac{XML} to formacja obiektów.
Obiekt składa się z klucza, atrybutów oraz innych obiektów.
Obok zaprezentowany jest przykładowy dokument $A_1$ o kluczu $A$,
z obiektami $B1$, $B2$, $G$, atrybutami $a_1$ oraz $a_2$.
Pozostałe symbole odnoszą się do zagnieżdżonych obiektów.
Duże litery oznaczają obiekty a małe atrybuty. Symbole $x_i$
odnoszą się do wartości skalarnych.

\columnbreak
\begin{figure}[H]
\footnotesize
\begin{tikzpicture}[scale=0.5]
	\draw[draw=black, thin, solid] (-6.00,5.20) rectangle (8.00,-5.80);
	\draw[draw=black, thin, solid] (3.00,-1.80) rectangle (0.00,-4.80);
	\draw[draw=black, thin, solid] (-1.00,-1.80) rectangle (-4.00,-4.80);
	\draw[draw=black, thin, solid] (-4.00,3.20) rectangle (-1.00,0.20);
	\draw[draw=black, thin, solid] (0.00,3.20) rectangle (3.00,0.20);
	\draw[draw=black, thin, solid] (-5.00,4.20) rectangle (7.00,-0.80);
	\node[black, anchor=south west] at (-6.06,5.25) {$A_1$};
	\node[black, anchor=south west] at (-0.06,3.25) {$C_2$};
	\node[black, anchor=south west] at (-4.06,3.25) {$C_1$};
	\node[black, anchor=south west] at (-4.06,-1.75) {$B_2$};
	\node[black, anchor=south west] at (-5.06,4.25) {$B_1$};
	\node[black, anchor=south west] at (-0.06,-1.75) {$G$};
	\node[black, anchor=south west] at (-3.56,2.25) {$c_1: x_1$};
	\node[black, anchor=south west] at (-3.56,1.25) {$c_b: x_2$};
	\node[black, anchor=south west] at (-3.56,0.25) {$c_r: x_3$};
	\node[black, anchor=south west] at (0.44,2.25) {$c_1: x_4$};
	\node[black, anchor=south west] at (0.44,0.25) {$c_3: x_5$};
	\node[black, anchor=south west] at (4.44,2.25) {$b_1: x_6$};
	\node[black, anchor=south west] at (-3.56,-2.75) {$b_1: x_7$};
	\node[black, anchor=south west] at (-3.56,-3.75) {$b_2: x_8$};
	\node[black, anchor=south west] at (-3.56,-4.75) {$b_3: x_9$};
	\node[black, anchor=south west] at (0.44,-2.75) {$g_1: x_{10}$};
	\node[black, anchor=south west] at (0.44,-3.75) {$g_2: x_{11}$};
	\node[black, anchor=south west] at (4.44,-2.75) {$a_1: x_{12}$};
	\node[black, anchor=south west] at (4.44,-3.75) {$a_2: x_{13}$};
\end{tikzpicture}
    \centering
    \caption{Przykładowy schemat danych}
    \label{przykład-danych}
\end{figure}
\end{multicols}

\D{datum-obj}{Obiekt}{struktura zawierająca klucz, atrybuty i obiekty.}
\D{datum-klucz}{Klucz}{identyfikator typu obiektu.}
\D{datum-attr}{Atrybut}{identyfikator wartości skalarnej w obiekcie.}
\D{datum-ścież}{Ścieżka}{ciąg kluczy, którymi należy się kierować 
                         aby dojść do wskazanego obiektu albo atrybutu 
                         przez zagłebianie się w podobiekty.}
\D{datum-typ}{Typ}{zbiór obiektów o identycznej ścieżce, 
                   co do którego należy oczekiwać obecności
                   atrybutów z ograniczonego zbioru kluczy oraz 
                   obiektów z ograniczonego zbioru typów.}

Na powyższym przykładzie (\cref{przykład-danych}) 
można zauważyć, 
że obiekty mogą różnić się od siebie 
pod względem obecności atrybutów oraz obiektów, 
mimo posiadania identycznych ścieżek.
W efekcie, 
przy przyjętej poniżej metodzie wyróżniania typów obiektów,
należy oczekiwać braków danych w niektórych obiektach.
"""

#lib
import lib.storage, lib.profile, lib.flow, lib.alias

#calc
import yaml, re

@lib.flow.make()
def Profiling(dir:str, kind:str, assignpath:str, aliaspath:str,  profargs:dict={}):

  r"""
    \subsubsection{Transformacja struktury}

  Transformacja ma na celu przetworzenie danych 
  z postaci zagnieżdżonych obiektów
  na dane tabelaryczne.
  Każdy obiekt jest przetwarzany 
  na wartość albo ciąg wartości,
  Atrybuty są wyłącznie pojedynczymi wartościami
  i wchodzą w skład ciągów wartości 
  utworzonych z obiektów.
  Obiekt jest ciągiem wartości o ile 
  występuje wielokrotnie w danej ścieżce.
  Jeśli tak nie jest to jego atrybuty
  definiją wartości w pierwszym nadrzędnym obiekcie,
  który jest ciągiem wartości.
  W skrócie: obiekty o ścieżkach, 
  które występują wielokrotnie
  tworzą ciągi wartości,
  a atrybuty i obiekty pojedyncze
  są tymi wartościami.

  Etapem pierwszym w implementacji
  jest wyróżnienie tych obiektów,
  które są ciągami wartości;
  etap drugi to zbieranie danych
  do tabel, gdzie każda tabela jest
  zbiorem ciągów wartości
  o identycznych ścieżkach.

  Obserwacje w tych tabelach zawierają
  unikalne identyfikatory przydzielane podczas procesu
  oraz identyfikatory tych ciągów, które były
  dla nich nadrzędne przed transformacją.
  W efekcie otrzymujemy relacyjną bazę danych.
  Dalsze sekcje zawierają opisy i przeglądy tych tabel.
  """

  P = lib.profile.Profiler( **profargs )

  kind = kind.upper()
  assert kind in ['JSON', 'JSONL', 'XML', 'HTMLMICRODATA']

  if kind == 'XML':
    P.XML(dir)
  elif kind == 'HTMLMICRODATA':
    P.HTMLmicrodata(dir)
  elif kind == 'JSON':
    P.JSON(dir)
  elif kind == 'JSONL':
    P.JSONl(dir, listname="data")

  H = P.dataframes()

  def pathnorm(x:str):
    x = re.sub(r'[^\w\.\-/\_]|\d', '', x)
    x = re.sub(r'\W+', '_', x)
    return x

  L = lib.alias.simplify(H, norm=pathnorm)
  H = { L['frames'][h0]: X.set_index(["id", "doc"])\
        .rename(columns=L['columns'][h0]) for h0, X in H.items() if not X.empty }

  L['columns'] = { L['frames'][h]: { v: k for k, v in Q.items() }  
                    for h, Q in L['columns'].items() }
  L['frames'] = { v: k for k, v in L['frames'].items() }
  with open(aliaspath, 'w') as f:
    yaml.dump(L, f, indent=2)#do wglądu

  A = { h: { k: None for k in V.keys() } for h, V in L['columns'].items() }
  with open(assignpath, 'w') as f:
    yaml.dump(A, f, indent=2)#do ręcznej edycji

  return lib.storage.Storage(dir, H)

from util import data as D

flow = { k: dict() for k in D.keys() }

flow['UPRP']['profiling'] = Profiling(D['UPRP']+'/raw/', kind='XML',
                                      assignpath=D['UPRP']+'/assignement.null.yaml', 
                                      aliaspath=D['UPRP']+'/alias.yaml').map(D['UPRP']+'/storage.pkl')

flow['Lens']['profiling'] = Profiling(D['Lens']+'/res/', kind='JSONL',
                                      assignpath=D['Lens']+'/assignement.null.yaml', 
                                      aliaspath=D['Lens']+'/alias.yaml').map(D['Lens']+'/storage.pkl')

flow['USPG']['profiling'] = Profiling(D['USPG']+'/raw/', kind='XML',
                                      profargs=dict(only=['developer.uspto.gov/grant/raw/us-patent-grant/us-bibliographic-data-grant/']),
                                      assignpath=D['USPG']+'/assignement.null.yaml',
                                      aliaspath=D['USPG']+'/alias.yaml').map(D['USPG']+'/storage.pkl')

flow['USPA']['profiling'] = Profiling(D['USPA']+'/raw/', kind='XML',
                                      profargs=dict(only=['developer.uspto.gov/application/raw/us-patent-application/us-bibliographic-data-application/']),
                                      assignpath=D['USPA']+'/assignement.null.yaml', 
                                      aliaspath=D['USPA']+'/alias.yaml').map(D['USPA']+'/storage.pkl')

flow['Google']['profiling'] = Profiling(D['Google']+'/web/', kind='HTMLmicrodata',
                                        assignpath=D['Google']+'/assignement.null.yaml', 
                                        aliaspath=D['Google']+'/alias.yaml').map(D['Google']+'/storage.pkl')