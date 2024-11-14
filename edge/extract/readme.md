Przeszukiwanie (Ekstrakcja)
===========================

Proces przeszukiwania ma na celu rozdzielenie danych tekstowych
na mniejsze części: sentencje, daty albo pojedyncze słowa - 
kawałki (ang. *chunks*).


```mermaid
---
title: Schemat ekstrakcji
---
graph LR
R[(raporty)]
--> A[podział interpunkcyjny]
--> S[(sentencje)]
--> Q@{ shape: processes, label: "wyszukiwania" } 
--> S
```