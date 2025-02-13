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

Dyfuzja wiedzy innowacyjnej w czasie i przestrzeni to proces
rozprzestrzeniania się nowych technologii pomiędzy podmiotami
w lokalnym otoczeniu.

Wiedza innowacyjna jest kluczowym elementem rozwoju gospodarczego.
To jej przypisuje się siłę napędową dla wzrostu gospodarczego
w nowoczesnych gospodarkach. Zrozumienie procesów dyfuzji 
wiedzy innowacyjnej może pozwolić na budowanie lepszych 
warunków dla rozwoju gospodarczego.

Patenty są dobrym wskaźnikiem tego zjawiska z 2 powodów:
po pierwsze dotyczą wyłacznie innowacji --- takie jest ich zadanie,
po drugie zawierają informacje na temat ich twórców, co pozwala
na stwierdzenie o ich lokalizacji, która jest kluczowym elementem
w procesie dyfuzji wiedzy.



\chapter{Przegląd literatury}\label{ch:intro}
\include{main}




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

  D = readdocstr(['main', 'grph', 'clst', 'endo', 'patt', 'rgst', 'subj', 'corr', 'rprt'])

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
    \begin{{document}}{x}\end{{document}}
    """

    with open('temp.tex', 'w') as f:
      f.write(include(y, root))

    TeX.PDF('temp.tex')
    TeX.PDF('temp.tex')

    shutil.move('temp.pdf', os.path.join(root, 'preview', f'{k0}.pdf'))

if __name__ == "__main__":
  main()