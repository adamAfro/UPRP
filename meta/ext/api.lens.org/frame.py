from pandas import DataFrame
from tqdm import tqdm as progress
import os, json

P, N = [], []
for f0 in progress(os.listdir('res')):
  with open(f'res/{f0}') as f: D = json.load(f)['data']
  for d in D:
    p, n = dict(), []
    
    p['country'] = d.get('jurisdiction')
    p['number'] = d.get('doc_number')
    p['lang'] = d.get('lang')
    p['suffix'] = d.get('kind')

    b = d.get('biblio', {})

    r = b.get("parties", {})
    if r is None: r = {}
    for o in r.get("applicants", []) + r.get("inventors", []):
      l = o.get("residence", None)
      u = o.get("extracted_name", {}).get("value", None)
      n.append({ "nameloc": l, "name": u, **p })

    p['publ'] = b.get('publication_reference', {}).get('date', None)
    p['appl'] = b.get('application_reference', {}).get('date', None)
    p['applno'] = b.get('application_reference', {}).get('doc_number', None)
    for t in b.get('invention_title', []):
      l = t.get("lang", "WO")
      p[f"{l.upper()}title"] = t.get("text", None)

    for m in d.get('families', {}).get('simple_family', {}).get("members", []):
      m = m["document_id"]
      p[m["jurisdiction"]] = m["doc_number"]

    for m in d.get('families', {}).get('extended_family', {}).get("members", []):
      m = m["document_id"]
      p[m["jurisdiction"] + "2"] = m["doc_number"]

    P.append(p)
    N.append(n)


P = DataFrame(P)
P['lang'] = P['lang'].str.upper()
P['country'] = P['country'].str.upper()
P = P.convert_dtypes()
P.to_csv('patents.csv', index=False)

N = DataFrame([n for V in N for n in V])
N['lang'] = N['lang'].str.upper()
N['country'] = N['country'].str.upper()
N = N.convert_dtypes()
N.to_csv('names.csv', index=False)