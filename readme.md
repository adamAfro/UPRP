# Patenty API.UPRP.GOV.PL

## Metadane (`/meta/`)

- dane pochodzące bezpośrednio z API, wstępnie w postaci `.XML`.

## Raporty (`/raport/`)

- dokumenty pochodzące z API, pobrane w postaci `.PDF` 
  na podstawie linków z metadanych.

Raporty to dokumenty dotyczące powiązań po między raportami,
które określali eksperci dziedzinowi. Każdy zawiera tabelę
z informacjami o wytworach powiązanych z analizowanym patentem.

### Proces wyciągania powiązań z raportów

1. Wyciąganie tekstów przy użyciu paddle OCR;
2. Konwersja do obrazów i przycinanie do odpowiednich fragmentów;
3. Wysyłanie framgentów do openAI-GPT-4o i odbiór obserwacji;
4. Wyciąganie ze zbioru obserwacji powiązań dotyczących wyłącznie
   polskich patentów i weryfikacja poprawności przy użyciu metadanych.