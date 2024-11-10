from pandas import DataFrame
from tqdm import tqdm as progress
from os import listdir, path
import xml.etree.ElementTree as ET

Y = []
with progress([f for f in listdir() if f.endswith(".xml")]) as F:
  e = '<?xml version="1.0" encoding="UTF-8"?>'
  for f0 in F:
    F.set_description(f0)
    with open(f0) as f: x = f.read()

    L = [l for u in x.split(e) for l in u.split('<?xml version="1.0"?>')]
    L = [l.replace("&", "?") for l in L]
    L = [l for l in L if not l.startswith("<!ENTITY")]
    L = [l for l in L if l.strip()]

    if f0.startswith('v2-'):
      for l in L:
        P = ET.fromstring(e + l)
        if P.tag != "us-patent-application": 
          raise Exception(P.tag)

        x = dict()

        B = P.find(".//us-bibliographic-data-application")
        A = B.find(".//application-reference/document-id")
        x['country'] = A.find(".//country").text
        x['number'] = A.find(".//doc-number").text
        x['appl'] = A.find(".//date").text
        x['applno'] = A.find(".//doc-number").text

        U = B.find(".//publication-reference/document-id")
        x['suffix'] = U.find(".//kind").text
        x['publ'] = U.find(".//date").text
        x['number'] = U.find(".//doc-number").text

        Y.append(x)

    elif f0.startswith('v1-'):
      for l in L:
        try: P = ET.fromstring(e + l)
        except: print(l.split('\n')[313])
        if P.tag != "patent-application-publication": 
          raise Exception(P.tag)
        
        x = dict(country="US")
        B = P.find(".//subdoc-bibliographic-information")
        U = B.find(".//document-id")
        x['number'] = U.find(".//doc-number").text
        x['suffix'] = U.find(".//kind-code").text
        x['publ'] = U.find(".//document-date").text

        A = B.find(".//domestic-filing-data")
        x['appl'] = A.find(".//filing-date").text
        
        NoA = A.find(".//application-number/doc-number")
        x['applno'] = NoA.text
        Y.append(x)

DataFrame(Y).to_csv("patents.csv", index=False)