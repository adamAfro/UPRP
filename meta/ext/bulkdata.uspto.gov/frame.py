from pandas import DataFrame, concat
from tqdm import tqdm as progress
from os import listdir, path
import xml.etree.ElementTree as ElementTree

# Mapowanie XML - Root
# --------------------
ET0 = dict()
ET0["us-patent-application"] = {
  "us-bibliographic-data-application/application-reference/document-id/country": "country",
  "us-bibliographic-data-application/publication-reference/document-id/doc-number": "number",
  "us-bibliographic-data-application/publication-reference/document-id/kind": "suffix",
  "us-bibliographic-data-application/application-reference/document-id/doc-number": "applno",
  "us-bibliographic-data-application/publication-reference/document-id/date": "publ",
  "us-bibliographic-data-application/application-reference/document-id/date": "appl",
  "us-bibliographic-data-application/invention-title": "ENtitle" }

ET0["patent-application-publication"] = {
  "subdoc-bibliographic-information/document-id/doc-number": "number",
  "subdoc-bibliographic-information/document-id/kind-code": "suffix",
  "subdoc-bibliographic-information/domestic-filing-data/application-number/doc-number": "applno",
  "subdoc-bibliographic-information/document-id/document-date": "publ",
  "subdoc-bibliographic-information/domestic-filing-data/filing-date": "appl",
  "subdoc-bibliographic-information/technical-information/title-of-invention": "ENtitle" }

# Mapowanie XML - Inventor/Applicant/Assignee
# -------------------------------------------
N0 = dict()
N0["us-bibliographic-data-application/us-parties/us-applicants/us-applicant"] = \
N0["us-bibliographic-data-application/us-applicants/inventors/inventor"] = \
N0["us-bibliographic-data-application/assignees/assignee"] = \
N0["us-bibliographic-data-application/inventors/inventor"] = \
N0["us-bibliographic-data-application/parties/applicants/applicant"] = \
N0["subdoc-bibliographic-information/inventors/inventor"] = \
N0["subdoc-bibliographic-information/inventors/first-named-inventor"] = \
N0["subdoc-bibliographic-information/assignee"] = {

  "orgname": "orgname",
  "organization-name": "orgname",

  "name/given-name": "first",
  "name/family-name": "last",  

  "addressbook/first-name": "first",
  "addressbook/last-name": "last",

  "addressbook/city": "city",
  "addressbook/state": "state",
  "addressbook/country": "location",
  
  "residence/residence-us/city": "city",
  "residence/residence-us/state": "state",
  "residence/residence-us/country-code": "country",
  
  "residence/residence-non-us/city": "city",
  "residence/residence-non-us/state": "state",
  "residence/residence-non-us/country-code": "country",

  "addressbook/address/city": "city",
  "addressbook/address/state": "state",
  "addressbook/address/country": "location",

  "nationality/country": "nationality",
  "residence/country": "residence" }

# Wczytywanie plików
# ------------------
D = []
for f0 in progress([f for f in listdir() if f.endswith(".xml")], desc="📂"):
  with open(f0) as f: D0 = f.read()

  D0 = D0.replace("&", "?")
  D0 = D0.split('<?xml version="1.0" encoding="UTF-8"?>')
  D0 = [d for d0 in D0 for d in d0.split('<?xml version="1.0"?>')]
  D0 = [d for d in D0 if d.strip()]
  D.extend(['<?xml version="1.0" encoding="UTF-8"?>\n' + d.strip() for d in D0])

# Przetwarzanie XML
# -----------------
P, N = [], []
for d in progress(D, desc="📄"):
  ET = ElementTree.fromstring(d)
  if ET.tag not in ET0: raise Exception(ET.tag)
  P.append({ k: ET.find(q).text for q, k in ET0.get(ET.tag, {}).items() })
  for k in N0.keys():
    for n0 in ET.findall(f".//{k}"):
      n = [(k, n0.find(q)) for q, k in N0[k].items()]
      n = { k: u.text for k, u in n if u is not None }
      N.append({ **P[-1], **n })

P = DataFrame(P)
P['country'] = P['country'].fillna("US")
P.to_csv("patents.csv", index=False)

N = DataFrame(N).replace("omitted", None)\
  .drop(columns=["ENtitle", "publ", "appl"])
N['country'] = N['country'].fillna("US")
N.query("~orgname.isna()").dropna(axis=1, how="all")\
  .drop(columns=['first', 'last'])\
  .rename(columns={"residence": "location"}).to_csv("org.csv", index=False)
N.query("orgname.isna()").dropna(axis=1, how="all")\
  .to_csv("names.csv", index=False)