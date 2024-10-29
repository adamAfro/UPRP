Metadane
========

Metadane dotyczącec raportów otrzymane w wyniku konwersji 
części danych z `XML` na dane tabelaryczne i dalszej obróbce
na skonkretyzowane dane tabelaryczne.

```mermaid
---
title: Pozyskiwanie metadanych
---
graph LR
G[(UPRP.GOV.PL)]
--> p[pobieranie] 
--> M0[(Meta.XML)]
--> r[parsowanie]
--> M[(Metadane)]
```

```mermaid
---
title: Struktura kluczy metadanych
---
graph LR;
N[(Wynalazcy)]
  N --- pN[/numer P./] -.-> cN
  N --- nN[/imiona/] --> cN
  N --- sN[/nazwiska/] --> cN
A[(Przypisanie)] -.- G[(Miasta)]
  A --- pA[/numer P./] -.-> cA
  A --- nA[/pełne imiona/] --> cA
  A --- sA[/organizacje/] --> cA
G[(Miasta)]
  G --- pG[/numer P./] -.-> cG[cięcie]
  G --- nG[/nazwa/] --> cG[cięcie]
T[(Tytuły)]
  T --- pT[/numer P./] -.-> cT
  T --- nT[/słowa/] --> cT
X[(Klucze)]
  X --- pX[/numer P./]
  X --- nX[/klucz/]
  cT[cięcie] --> X[(Klucze)]
  cN[cięcie] --> X[(Klucze)]
  cA[cięcie] --> X[(Klucze)]
  cG[cięcie] --> X[(Klucze)]
```

```mermaid
---
title: Struktura numeracji metadanych
---
graph LR
P[(Numery P.)]
P --- pP[/numer P./]
P --- nP[/numer prawa wyłączności/]
```

```mermaid
---
title: Struktura datowania metadanych
---
graph LR
D[(Daty.)]
D --- pD[/numer P./]
D --- dD[/dzień/]
D --- mD[/miesiąc/]
D --- yD[/rok/]
```