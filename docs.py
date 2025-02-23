r"""
\documentclass[12pt, withmarginpar]{mwbk}
\include{defs}
\begin{document}




\thispagestyle{empty}%brak numeracji


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%tytuły definiuje jako makrodefinicje, gdyż zamierzam je%%%
%%%%%powtórzyć na stronie ze streszczeniami, to nic nie boli%%%
%%%%%a gwarantuje, że będą one takie same, i~tak ma być.%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%P.Wlaź
\newcommand\tytul{Dyfuzja wiedzy innowacyjnej w czasie i przestrzeni
                  na podstawie patentów z ostatnich dziesięciu lat}

\newcommand\tytulangielski{Diffusion of innovative knowledge 
                           in time and space based on patents 
                           from the last ten years}

\noindent
\hspace*{-3mm}\includegraphics[width=8.67cm]{logo.pdf}
\fontfamily{qhv}\fontsize{12pt}{15pt}\selectfont

\vfil 
\noindent Katedra Matematyki Stosowanej

\vfil\vfil\vfil\vfil


\fontsize{40pt}{50pt}\selectfont
\noindent Praca inżynierska

\fontsize{12pt}{15pt}\selectfont


\vfil
\noindent
na kierunku \emph{inżynieria i~analiza danych}
\vfil\vfil

\vspace{2cm}
\fontsize{16pt}{18pt}\selectfont
\noindent \tytul

\vspace{1cm}
\fontsize{16pt}{18pt}\selectfont
\noindent \tytulangielski

\vfil\vfil\vfil\vfil
\fontsize{16pt}{20pt}\selectfont

\noindent 
Adam Jakubczak

\vfil
\fontsize{12pt}{15pt}\selectfont
\noindent
numer albumu: 098750


\vfil

\noindent
promotor: dr inż. Korneliusz Pylak

\vfil\vfil\vfil

\fontsize{9pt}{12pt}\selectfont

\noindent
Lublin 2025

\normalsize \rm





\tableofcontents
\chapter*{Wstęp}

Dyfuzja wiedzy innowacyjnej w czasie i przestrzeni 
  to proces przepływu informacji 
    o nowych 
      technologiach, 
      produktach,
      usługach, czy 
      procesach 
    pomiędzy różnymi podmiotami.
  Jest kluczowym elementem rozwoju gospodarczego.
  To jej przypisuje się siłę napędową wzrostu gospodarczego
    w nowoczesnych gospodarkach. 
Zrozumienie procesów dyfuzji wiedzy innowacyjnej 
  może pozwolić na budowanie lepszych warunków 
    rozwoju gospodarczego.

Patenty są dobrym wskaźnikiem tego zjawiska z 2 powodów:
  po pierwsze dotyczą wyłącznie innowacji.
  Po drugie zawierają kluczowe informacje w dyfuzji wiedzy innowacyjnej,
    to jest:
      innowacyjność (tylko patenty innowacyjne dostają ochronę),
      czas powstania (data złożenia wniosku patentowego) oraz
      miejsce położenia autorów.

Niniejsza praca jest przeglądem danych urzędu patentowego,
  która przy użyciu technik opisowych
  ma przybliżyć zjawisko dyfuzji wiedzy innowacyjnej
  w Polsce.



\chapter{Przegląd literatury}\label{ch:intro}
\include{main}




    \newpage\section*{Objaśnienia skrótów}
  \begin{acronym}
\acro{UPRP}{Urząd Patentowy Rzeczypospolitej Polskiej}
\acro{EPO}{European Patent Office}
\acro{WIPO}{World Intellectual Property Organization}
\acro{MKP}{Międzynarodowa Klasyfikacja Patentów}
\acro{IPC}{International Patent Classification}
\acro{API}{Application Programming Interface}
\acro{URI}{Uniform Resource Identifier}
\acro{URL}{Uniform Resource Locator}
\acro{OCR}{Optical Character Recognition}
\acro{XML}{Extensible Markup Language}
\acro{PDF}{Portable Document Format}
\end{acronym}



  \chapter{Dane}

W Polsce 
  centralnym organem 
    odpowiedzialnym za przyznawanie patentów 
    jest \acf{UPRP}. 
Oprócz 
  ochroną patentową oraz 
  publikowaniem informacji o patentach, 
  urząd prowadzi bazę danych patentów, 
    która jest dostępna publicznie 
      przy użyciu \ac{API}.
Pozwala to na automatyczne pobieranie danych 
  przy pomocy skryptów.
Niniejszy rozdział opisuje 
  wyłącznie pozyskiwania danych 
  i przetwarzania ich do postaci, 
    która może być użyta w dalszym wnioskowaniu.

  \include{prfl}

  \include{patt}

  \include{rprt}

  \include{rgst}

  \include{subj}


  \chapter{Analiza}

Dyfuzja wiedzy 
  jest procesem 
    zachodzącym między osobami.
  Cytowania patentowe 
    są symptomem 
      tego procesu.
  Dla uproszczenia, 
    nazywamy je \emph{przepływem}.
      Dotyczy on transferu wiedzy 
        między osobami 
          pełniącymi rolę patentowe
            w jednym z patentów 
              powiązanych cytowaniem.
Każde cytowanie jest więc relacją 
  między dwoma osobami.
  Relacja ta nie jest symetrzyczna:
    jedna jest osobą cytująca, 
    a druga cytowaną.

  \include{grph}

  \include{difp}

  \include{difw}


    \chapter{Podsumowanie}

Celem pracy jest przybliżenie zjawiska dyfuzji wiedzy innowacyjnej 
w Polsce z wykorzystaniem danych urzędu patentowego i technik opisowych. 
Dyfuzja wiedzy innowacyjnej, czyli przepływ informacji o nowych technologiach, 
produktach i usługach, jest kluczowym elementem rozwoju gospodarczego. 
Patenty stanowią dobry wskaźnik tego zjawiska, ponieważ dotyczą wyłącznie innowacji 
i zawierają kluczowe informacje, takie jak innowacyjność, czas powstania i miejsce autorów.

W pracy wykorzystano dane z Urzędu Patentowego Rzeczypospolitej Polskiej.
Analiza obejmowała metadane patentów, wydarzenia związane z patentami, klasyfikacje patentowe 
(\ac{IPC}) oraz dane lokalizacyjne. Wykorzystano również raporty o stanie techniki. 
Do identyfikacji osób i uzupełniania braków geolokalizacji 
zastosowano metody podobieństwa afiliacyjno-geolokalizacyjnego i nazewniczego.
Wyniki. Analiza przepływu wiedzy opiera się na grafie 
skierowanym relacji między osobami, tworzonym na podstawie 
cytowań w raportach o stanie techniki. 
Badanie przedstawia rozkład opóźnienia cytowań patentowych, 
odległości między osobami cytującymi, a cytowanymi oraz 
charakterystykę lokalizacji na podstawie ilości osób cytujących i cytowanych. 

Analizowa obejmuje dyfuzję wiedzy innowacyjnej w powiatach i województwach, 
uwzględniając pochodzenie cytowań.

Z analizy wynika, że dyfuzja wiedzy innowacyjnej 
w Polsce jest nierównomierna przestrzennie, 
z wyraźną koncentracją w powiatach miejskich. 
Istotną rolę odgrywają duże miasta, takie jak Warszawa, Kraków, Wrocław i Poznań. 
Większość cytowań zachodzi między osobami z tej samej lokalizacji, 
a odległość między osobami cytującymi a cytowanymi.
W Polsce istnieje klaster innowacyjny obejmujący województwa
mazowieckie, małopolskie i śląskie, dolnośląskie, łódzkie i wielkopolskie.
Wymiana informacji w obszarze tych województw jest szczególnie widzoczna.

Praca ma pewne ograniczenia związane z jakością i dostępnością danych. 
Nie wszystkie wynalazki są opatentowane, a dane lokalizacyjne 
są ograniczone do nazwy miejscowości. Normalizacja nazw miejscowości 
i identyfikacja osób stanowią wyzwanie ze względu na niejednoznaczność danych. 
Wykluczenie wsi z analizy mogło wpłynąć na wyniki.

W przyszłych badaniach warto rozważyć wykorzystanie innych źródeł danych, 
takich jak publikacje naukowe i dane o współpracy między firmami. 
Można również zastosować bardziej zaawansowane metody analizy sieciowej 
i modelowania przestrzennego, aby lepiej zrozumieć procesy dyfuzji wiedzy innowacyjnej. 
Interesujące byłoby również zbadanie wpływu polityki innowacyjnej 
na dyfuzję wiedzy oraz porównanie wyników z innymi krajami.

\bibliographystyle{unsrt}\bibliography{cit}

\listoffigures

\listoftables

  \chapter*{Załączniki}
\begin{enumerate}
\item Płyta CD z niniejszą pracą w wersji elektronicznej.
\end{enumerate}

  \chapter*{Streszczenie (Summary)}

\bigskip

\bigskip

\begin{center}
  \textbf{\tytul}
\end{center}

Analiza przedstawia dyfuzję wiedzy innowacyjnej w Polsce 
w oparciu o dane patentowe z ostatnich dziesięciu lat. 
Wykorzystane są dane z Urzędu Patentowego Rzeczypospolitej Polskiej, 
w tym raporty o stanie techniki, 
do zbadania przepływu wiedzy między osobami i organizacjami. 
Analiza obejmuje identyfikację osób, ich lokalizację, 
klasyfikacje patentowe oraz relacje cytowań między patentami. 
Praca bada rozkład opóźnienia cytowań patentowych, odległości 
między osobami cytującymi i cytowanymi oraz charakterystykę 
lokalizacji pod względem ilości 
osób zaangażowanych w proces dyfuzji.
 Wyniki wskazują na nierównomierne rozłożenie 
 dyfuzji wiedzy w Polsce, 
 z koncentracją w kilku kluczowych powiatach i województwach.

\bigskip

\begin{center}
  \textbf{\textit{\tytulangielski}}
\end{center}

\selecthyphenation{english}
{\it 
The analysis presents the diffusion of innovative knowledge in Poland based on patent data from the last ten years. Data from the Polish Patent Office, including prior art reports, are used to examine the flow of knowledge between individuals and organizations. The analysis includes the identification of individuals, their locations, patent classifications, and citation relationships between patents. The study investigates the distribution of patent citation delays, distances between citing and cited individuals, and the characteristics of locations in terms of the number of people involved in the diffusion process. The results indicate an uneven distribution of knowledge diffusion in Poland, with concentration in a few key counties and provinces.

}

\end{document}
"""

import os, shutil, subprocess, datetime, ast, re

def exec(command):
  return subprocess.run(command, shell=True, text=True)

class TeX:

  @staticmethod
  def PDF(file:str):
    exec(f'pdflatex -interaction=nonstopmode {file}')

  @staticmethod
  def PDFbib(file:str):

    TeX.PDF(file)

    exec(f'bibtex {file.split(".")[0]}')

    TeX.PDF(file)
    TeX.PDF(file)

def readdocstr(files:list[str]):

  Y = dict()

  for p in files:

    with open(p+'.py', 'r') as f:

      T = ast.parse(f.read())
      x = ast.get_docstring(T)
      Y = {**Y, p: x }

      for n in ast.walk(T):
        if isinstance(n, ast.FunctionDef):
          k = n.name
          d = ast.get_docstring(n)
          Y = {**Y, f'{p}': (Y.get(p, '') or '') + r'\n\n' + (d or '') }

  return Y

def include(x:str, root):

  q = re.compile(r'\\include{([^}]+)}')
  M = q.findall(x)

  for m in M:
    p = os.path.join(root, f'{m}.tex')
    if os.path.exists(p):
      with open(p, 'r') as f: y = f.read()
      x = x.replace(rf'\include{{{m}}}', y)

  return x

def main():

  D = readdocstr(['main', 'prfl', 'grph', 'patt', 'rgst', 'subj', 'rprt', 'difp', 'difw'])

  root = os.path.dirname(os.path.abspath(__file__))
  wd = './workdir'
  shutil.copy('cit.bib', os.path.join(wd, 'cit.bib'))
  os.makedirs(wd, exist_ok=True)
  os.chdir(wd)

  if (len(os.sys.argv) == 1) or (os.sys.argv[1] == ''):

    y = __doc__

    for k, d in D.items():
      if d is None: continue
      y = y.replace(rf'\include{{{k}}}', d)

    y = include(y, root)
   #../workdir
    y = re.sub(r'(\\includegraphics\[.*?\]{)(.*?)(})', r'\1../\2\3', y)
    y = re.sub(r'(\\input{)(.*?)(})', r'\1../\2\3', y)

    with open('main.tex', 'w') as f: f.write(y)

    TeX.PDFbib('main.tex')

    D0 = datetime.datetime.now().strftime('%F')
    fY = f'Praca-Dyplomowa-Jakubczak-{D0}.PDF'

    shutil.move('main.pdf', os.path.join(root, fY))

  else:

    k0 = os.sys.argv[1]

    x = { k: v for k, v in D.items() if k.startswith(k0) }
    x = '\n\n'.join(v for k, v in x.items() if v is not None)

    y = rf"""
    \documentclass[12pt, withmarginpar]{{mwbk}}
    \include{{defs}}
    \begin{{document}}

    {x}

    \end{{document}}
    """

    y = y.replace(r'{fig/', r'{../fig/')
    y = y.replace(r'{tbl/', r'{../tbl/')
    with open('temp.tex', 'w') as f:
      f.write(include(y, root))

    TeX.PDF('temp.tex')
    TeX.PDF('temp.tex')

    shutil.move('temp.pdf', os.path.join(root, 'preview', f'{k0}.pdf'))

if __name__ == "__main__":
  main()