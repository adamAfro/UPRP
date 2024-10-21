import json
with open("XML.paths.json") as f:
  pathsmeta = json.load(f)

def relate(node, datadict, P:str,  suppath="XML", supid=0):
  path = f"{suppath}/{node.tag}"
  if path not in datadict: datadict[path] = []
  entry = dict(P=P, PID=supid, ID=len(datadict[path]))
  datadict[path].append(entry)
  
  for attr in node.attrib: entry['&' + attr] = node.attrib[attr]
  if (node.text is not None) and (len(node.text.strip()) > 0):
    entry['$'] = node.text.strip()
  
  nodes = list(node)
  if len(nodes) == 0: return
  
  for child in node:
    subpath = f"{path}/{child.tag}"
    subpathmeta = pathsmeta[subpath]
    if len(subpathmeta['attrs']) == 0:
      if subpathmeta['text'] == True:
        if subpathmeta['plular'] == False:
          entry['$' + child.tag] = child.text.strip() if child.text is not None else "~"
          continue

    relate(child, datadict, P, path, entry['ID'])

from os import path, makedirs
from pandas import DataFrame
from tqdm import tqdm as progress
import xml.etree.ElementTree as ET
from os import listdir, path
F = [path.join("raw", f) for f in listdir("raw")]
datadict = dict()
for p in progress(F):
  with open(p) as f: x = f.read()
  y = ET.fromstring(x)
  P = p.split("/")[-1].replace("P.", "").replace("_empty", "").replace(".xml", "")
  relate(y, datadict, P)
for k in datadict:
  df = DataFrame(datadict[k])
  dir = path.join("frames", k)
  makedirs(dir, exist_ok=True)
  df.to_csv(path.join(dir, f"df.csv"), index=False)