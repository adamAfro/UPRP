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
  To jej przypisuje się siłę napędową dla wzrostu gospodarczego
    w nowoczesnych gospodarkach. 
Zrozumienie procesów dyfuzji wiedzy innowacyjnej 
  może pozwolić na budowanie lepszych warunków dla 
    rozwoju gospodarczego.

Patenty są dobrym wskaźnikiem tego zjawiska z 2 powodów:
  po pierwsze dotyczą wyłącznie innowacji.
  Po drugie zawierają kluczowe informacje w dyfuzji wiedzy innowacyjnej,
    to jest:
      innowacyjność (tylko patenty innowacyjne dostają ochronę),
      czas powstania (data złożenia wniosku patentowego) oraz
      miejsce położenia autorów.

Praca ta jest wglądem w dane urzędu patentowego,
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

  \include{prfl:Profiling}

    \include{patt}

  \include{patt:code}

  \include{patt:event}

  \include{patt:geolocate}

  \include{patt:classify}


    \include{rprt}

  \include{rprt:Indexing}

  \include{rprt:Parsing}

  \include{rprt:Narrow}

  \include{rprt:edges}



    \include{rgst}

  \include{rgst:Nameclsf}

  \include{rgst:Pull}

  \include{rgst:Textual}

  \include{rgst:Spacetime}

    \include{subj}

  \include{subj:affilgeo}

  \include{subj:affilnames}

  \include{subj:simcalc}

  \include{subj:identify}

  \include{subj:fillgeo}


  \chapter{Analiza}

    \include{grph}

  \include{grph:network}

  \include{grph:distcart}

  \include{grph:distplot}

  \include{grph:distplotyear}


    \include{difu}

  \include{grph:ncited}

  \include{grph:mxtrwoj}

  \include{grph:distplot}

  \include{grph:distplotyear}


\bibliographystyle{plain}\bibliography{cit}

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

\bigskip

\begin{center}
  \textbf{\textit{\tytulangielski}}
\end{center}

\selecthyphenation{english}
{\it }

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
          Y = {**Y, f'{p}:{k}': d }

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

  D = readdocstr(['main', 'prfl', 'grph', 'patt', 'rgst', 'subj', 'rprt', 'difu'])

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
    # y = y.replace(r'{fig/', r'{../fig/')

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
    with open('temp.tex', 'w') as f:
      f.write(include(y, root))

    TeX.PDF('temp.tex')
    TeX.PDF('temp.tex')

    shutil.move('temp.pdf', os.path.join(root, 'preview', f'{k0}.pdf'))

if __name__ == "__main__":
  main()