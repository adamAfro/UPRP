Metadane
--------

Metadane dotyczącec raportów otrzymane w wyniku konwersji 
części danych z `XML` na dane tabelaryczne i dalszej obróbce
na skonkretyzowane dane tabelaryczne.

Struktura
=========

```mermaid
graph LR;

P[(Numery P.)]
P --- pP[/numer P./]
P --- nP[/numer alternatywny/]

D[(Daty.)]
D --- pD[/numer P./]
D --- dD[/dzień/]
D --- mD[/miesiąc/]
D --- yD[/rok/]

N[(Wynalazcy)]
N --- pN[/numer P./]
N --- nN[/imiona/]
N --- sN[/nazwiska/]

pN -.-> cN[cięcie]
nN --> cN[cięcie]
sN --> cN[cięcie]
cN --> X[(Klucze)]

A[(Przypisanie)]
A --- pA[/numer P./]
A --- nA[/pełne imiona/]
A --- sA[/organizacje/]

A -.- G[(Miasta)]
G --- pG[/numer P./]
G --- nG[/nazwa/]

pG -.-> cG[cięcie]
nG --> cG[cięcie]
cG --> X[(Klucze)]

pA -.-> cA[cięcie]
sA --> cA[cięcie]
nA --> cA[cięcie]
cA --> X[(Klucze)]

T[(Tytuły)]
T --- pT[/numer P./]
T --- nT[/słowa/]

pT -.-> cT[cięcie]
nT --> cT[cięcie]
cT --> X[(Klucze)]

X --- pX[/numer P./]
X --- nX[/klucz/]
```