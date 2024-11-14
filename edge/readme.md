Raporty
=======



Pozyskiwanie danych ze skanów dokumentów
----------------------------------------

Dane z raportów w formacie *PDF* zostają wyciągnięte, za pomocą
multimodalnego modelu językowego, do postaci obserwacji tekstowych.
Wyrażenia regularne wyciągają kody i daty, ~~reszta jest dzielona
na słowa. Późniejsze parowanie w.w. ma na celu walidację 
samych kodów.~~ 🛠

```mermaid
---
title: Schemat procesu wyciagania danych
---
graph TB
M[(Metadane)] --> p
G[(UPRP.GOV.PL)]
--> p[pobieranie]
--> PDF[(Dokumenty)]
--> OCR[Paddle-OCR-v4-pl]
--> OCV[OpenCV.crop]
--> GPT[OpenAI-GPT-4o-mini]
--> R0[(Powiązania+)]
```



Wyszukiwanie wierzchołków
-------------------------

🛠



Weryfikacja wyszukiwania
------------------------

🛠