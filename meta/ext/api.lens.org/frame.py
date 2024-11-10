from pandas import DataFrame
import os, json

Y = []
for f0 in os.listdir('res'):
  with open(f'res/{f0}') as f: D = json.load(f)['data']
  for d in D:
    x = dict()
    
    x['country'] = d.get('jurisdiction')
    x['number'] = d.get('doc_number')
    x['lang'] = d.get('lang')
    x['suffix'] = d.get('kind')
    
    b = d.get('biblio', {})
    
    x['publ'] = b.get('publication_reference', {}).get('date', None)
    x['appl'] = b.get('application_reference', {}).get('date', None)
    x['applno'] = b.get('application_reference', {}).get('doc_number', None)
    x['claim'] = b.get('priority_claims', {}).get('earliest_claim', {}).get('date', None)
    for t in b.get('invention_title', [{}]):
      if t.get("lang", None) == "en":
        x['title'] = t.get("text", None)
    
    for m in d.get('families', {}).get('simple_family', {}).get("members", []):
      m = m["document_id"]
      x[m["jurisdiction"]] = m["doc_number"]
    
    for m in d.get('families', {}).get('extended_family', {}).get("members", []):
      m = m["document_id"]
      x[m["jurisdiction"] + "2"] = m["doc_number"]

    Y.append(x)


Y = DataFrame(Y)
Y['lang'] = Y['lang'].str.upper()
Y['country'] = Y['country'].str.upper()
Y = Y.convert_dtypes()
Y.to_csv('patents.csv', index=False)