# Pobieranie danych dot. patentów z lens.org
# ==========================================
from pandas import read_csv, DataFrame
from tqdm import tqdm as progress; progress.pandas()
import requests, json, datetime, os, time

# Funkcja API do pobierania przyjmuje kody patentowe pod
# ustaloną jurysdykcja - oznaczana przez dwuliterowy kod.
# Zapytania i dane są zapisywane w formacie JSON do późniejszych
# analiz.
def API(j:str, N:list, t='puttokenhere'):
  
  q = f'{{ "size": 100, "query": "(jurisdiction:{j}) AND ({" OR ".join(["(doc_number:"+n+")" for n in N])})" }}'
  h = {'Authorization': f'Bearer {t}', 'Content-Type': 'application/json'}
  u = 'https://api.lens.org/patent/search'
  y = requests.post(u, data=q, headers=h)
  if y.status_code != 200: raise Exception(f'Error {y.status_code} {y.text}')    
  t = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
  with open(f'res/{t}.json', 'w') as f: f.write(y.text)
  with open(f'req/{t}.json', 'w') as f: 
    f.write(json.dumps({ "jurisdiction": j, "doc_number": N }))
  return json.loads(y.text)

# Do pobrania patentów wystarczajace są ich same kody.
X = read_csv("../patent.csv", dtype=str)[["country", "number", "sufix"]]\
 .drop_duplicates(subset=["country", "number"]).dropna()

# Pominięcie patentów USA
# -----------------------
# Urząd patentowy USA oferuje swoje dane od 2001 roku, oraz
# całościowe w postaci ogólnodostępnych plików. Z tego powodu
# są pomijane w zapytaniach.
# Z podobnych względów mogą być pomijane inne kraje,
# co do których brak pewności o isteniniu takiej bazy (CN, JP).
X = X.query('~country.str.startswith("US")')
X = X.query('~country.str.startswith("CN")')
X = X.query('~country.str.startswith("JP")')


# Odfiltrowanie patentów polskich
# --------------------------------
#
# Ze względu na to, że polskie patenty są dostępne w
# bazie UPRP to są pomijane w żądaniach.
X = X.query('~country.str.startswith("PL")')

# Wybór zbiorów
# -------------
#
# Ze względu na liczność zbiorów, wybierane są tylko
# te, które mają więcej niż 1000 patentów. Inne zbiory mogą
# być błędne, albo i nie. W każdym razie ich liczność jest
# argumentem przeciw temu, dlatego na wstępie są poimjane.
c = X['country'].value_counts().to_frame()
c.query('1000<=count').plot.barh()
c.query('100<=count<1000').plot.barh()
c.query('10<=count<100').plot.barh()
c.query('1<=count<10').plot.barh()
X = X.query('country in @c.query("1000<=count").index')

# Ograniczenia API
# ---------------
#
# Lens.org zazwyczaj zwraca zazwyczaj więcej niż 100 wyników,
# a ograniczenie zwrotne zapytania to właśnie 100.
# Z tego powodu, później należy jeszcze raz przetestować, czy
# próby pobrania wiązały się z porażką z powodu limitu API,
# czy braku danych.

# Pobrane dane
# ------------
#
# Przed pobieraniem trzeba sprawdzić co zostało już pobrane,
# i co w ogóle było poddane próbie pobierania. Po filtracji
# pobierana jest reszta, dzięki temu oszczędzane są ograniczone
# zasoby API.
# Czasami jest wiele znajd dla pojedynczego kodu, bo są różne
# rodzaje dokumentów. Dodanie jednak typu dokumentu, który nie
# zawsze jest obecny albo poprawny wiąże się z problemem braku
# znajd właśnie z tych powodów.
#
# Niektóre patenty są pominiete z powodów wymienionych wcześniej.
# Mając odpowiedzi z pierwszej iteracji, które mają
# liczność `result` większą/równą całości `total` wiadomo, 
# że obecne w niej nieznajdy są poprawnie nieobecne.
# Jeśli jest `results < total`, wtedy należy powtórnie 
# pobierać dane, bo `results < 100`.
Q = []
for f0 in os.listdir('req'):
  with open(f'req/{f0}') as f: q = json.load(f)
  if f0.endswith('0-res.json'):
    for n in q['doc_number']:
      Q.append({ "country": q['jurisdiction'], "number": n, "matches": 0 })
    continue
  with open(f'res/{f0}') as f: R = json.load(f)
  for n in q['doc_number']:
    k = sum([1 for d in R['data'] if (n == d['doc_number'])
              and (q['jurisdiction'] == d['jurisdiction'])])
    if (k == 0) and ((R.get('results', 0) == 0) or (R['results'] < R['total'])): continue
    Q.append({ "country": q['jurisdiction'], "number": n, "matches": k })
Q = DataFrame(Q)
Q['matches'].value_counts().sort_index()\
  .plot.bar(title='Wyszukiwane kody patentowe', 
            ylabel="ilość wyszukiwanych kodów patentowych",
            xlabel="ilość dopasowań")

# Wcześniejsze błędy doporwadziły do nieporawnych duplikatów,
# które należy usunąć.
Q = Q.sort_values('matches', ascending=False).drop_duplicates(keep='first')

X = X.merge(Q, on=['country', 'number'], how='left')
X['matches'].fillna(-1).astype(int).astype(str).replace("-1", "N/A")\
  .value_counts().sort_index()\
  .plot.bar(title='Wyniki zapytań API',
            ylabel="ilość wyszukiwanych kodów patentowych",
            xlabel="ilość dopasowań");

X0 = X
X = X.query('matches.isnull()').drop('matches', axis=1)

# Pobieranie patentów
# -------------------
# Ograniczenie pobierania do `k` ze względu na limity API:
# limit `k` powinien zapewnić w większosci przypadków margines
# na ewentualne nadmiarowe pobrania, a dzięki temu można później
# określić, czy jakieś inne dane zostały pominięte. Ustalenie
# `k` opiera się o proste obliczenia - automatycznie dąży do 
# zachowania 100 rekordów w odpowiedziach na podst. wcz. zap.
#
# Dodadkowe kroki:
#
# * wybranie arbitralnie odpowiedniej długości kodów patentowych:
# różne kraje mają różne długości, a pośród nich jest wiele odstępstw;
# * ustalenie jurysdykcji.
for c in X0['country'].value_counts().sort_values(ascending=True).index:
  for l in X0.query('country == @c')['number'].str.len().value_counts()\
    .sort_values(ascending=False).index:
    
    C = X.query('(country == @c) and (number.str.len() == @l)')
    print(c, l, f'{Q[Q["country"] == c].shape[0]} 💾')
    dt = 60/10 # maks. 10 zap. na min.
    if C.shape[0] < 75: continue
    with progress(total=C.shape[0]) as p:
      i, n, m5 = 0, 75, [100, 100, 100]
      while i < C.shape[0]:
        t0 = time.time()
        B = C.iloc[i:i+n]
        R = API(c, [v for v in B['number'].values])
        dt0 = time.time() - t0
        if dt0 < dt: time.sleep(dt - dt0 + 0.1)

        p.update(n)
        i += n

        if R.get('results', 0) == 0:
          print('📐💔')
          with open(f'req/{datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}-0-res.json', 'w') as f: 
            f.write(json.dumps({ "jurisdiction": c, "doc_number": [n for n in C['number']] }))
          break
        m5 = [m5[1], m5[2], R['results']]
        n = int(n + (95 - sum(m5)/len(m5))//5)
        p.set_postfix({'n': n, 'total': R['total'], 'results': R['results'] })
        