Cytowania raportów UPRP
=======================

Cel: graf czasoprzestrzenny powiązań między wynalazcami,
a wynalazcami i naukowcami, na podstawie ekspertyz[^edge/readme.md]
Urzędu Patentowego Rzeczypospolitej Polskiej.
Krawędź ważona grafu mówi o tym, że w wierzchołki (wynalazcy/naukowcy)
posiadają podobną wiedzę, a wagę wyznacza suma wartości każdego powiązania
złożenia daty i położenia w przestrzeni.

Ekspertyzy to raporty wykonywane przez ekspertów.
Pracownicy urzędu mają podważyć innowacyjność zgłoszonych patentów 
poprzez odniesienia do publikacji na temat podobnych rozwiązań.



Ograniczenia danych z raportów
------------------------------

**Nie każde odniesienie jest poddane analizie**.
Każdy raport odniesień do publikacji. Publikacje odnoszą sie m.in.
do patentów, publikacji naukowych, stron internetowych, artykułów prasowych, 
książek itp. **Część odniesień jest odrzucana**.

**Odniesienia nie zawierają potrzebnych informacji o przestrzeni**.
Najkrótszym odniesieniem jest numer patentu, niezależnie od typu 
można oczekiwać też daty, a w optymistycznym przypadku 
tytułu, autorów oraz organizacji, która stoi za opublikowaną pracą.
Raporty dostarczają wyłącznie informacji o tym, że powiązanie istnieje,
najczęściej też o czasie publikacji, ale nie dostarczają informacji o
położeniu przestrzennym. **Żeby je uzyskać trzeba skorzystać z innych źródeł**
[^node/readme.md].



Słownik
-------

- **wierzchołek** - osoba, która zgłosiła patent / autor publikacji;
- **krawędź** - mówi o tym, że wierzchołki posiadają podobną wiedzę;
- **wiedza** - wielowymiarowa wartość;
- **podobna/wspólna wiedza** - pojedynczy wymiar wiedzy, który jest
  wagą krawędzi 2 wierzchołków;
- **metadane**: dane z UPRP - 2 role: 
  pobieranie raportów / dane o wynalazcach i przetwarzanie.
- **raport** - ekspertyza Urzędu Patentowego;
- **publikacja** - praca naukowa albo patent;
- **odniesienie** - wpis w raporcie, który odnosi się do publikacji, 
  czasami nazywany **cytowanie**; odniesienia mają różne typy:
  - **poprawne** - odniesienia na kartkach raportów które zawierają 
    odniesienie do publikacji/patentu, który miał na myśli ekspert;
  - **pominięte** - kody niepożądanie pominięte na różnych 
    etapach prcesu; [^edge/readme.md]
  - **niepoprawne** - błędnie rozpoznane numery patentów/daty,
    w wyniku błędów modelu, albo wyciągania tekstu; [^edge/readme.md]
  - **mylące** - błędnie rozpoznane numery patentów/daty,
    w wyniku błędów modelu, albo wyciągania tekstu,
    które wskazują na inną prawdziwą publikację; [^edge/readme.md]



Dane
----

Raporty[^edge/readme.md] dostarczają informacji o powiązaniach (krawędzie),
a dodatkowe źródła[^node/readme.md] dostarczają informacji o położeniu 
w czasie i przestrzeni (wierzchołki).

🏗 **Uwagi na dalsze prace**:

* jeśli na kolejnych etapach wystąpią problemy z pozyskiwaniem danych
  zewnętrznych można skorzystać z danych z raportów o ile występują.


[^edge/readme.md]: Pozyskiwanie raportów: [`/edge`](edge/readme.md)
[^node/readme.md]: Dodatkowe źródła danych: [`/node`](node/readme.md)