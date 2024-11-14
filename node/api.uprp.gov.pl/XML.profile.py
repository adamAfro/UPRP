def profile(node, refdict, path="XML", sep="/"):
  path = f"{path}{sep}{node.tag}"
  if path not in refdict:
    refdict[path] = dict(attrs=set(), text=False, plular=False)
  if node.attrib is not None:
    for attr in node.attrib:
      refdict[path]['attrs'].add(attr)

  if (node.text is not None) and (len(node.text.strip()) > 0):
    refdict[path]['text'] = True
  if list(node):
    for child in node:
      profile(child, refdict, path)
    counts = dict()
    for child in node:
      if child.tag not in counts:
        counts[child.tag] = 0
      counts[child.tag] += 1
    for tag in counts:
      if counts[tag] > 1:
        refdict[f"{path}{sep}{tag}"]['plular'] = True

from os import listdir, path
from tqdm import tqdm as progress
import xml.etree.ElementTree as ET

paths = {}
F = [path.join("raw", f) for f in listdir("raw")]
for p in progress(F):
  with open(p) as f: x = f.read()
  y = ET.fromstring(x)
  profile(y, paths)

import json
with open("XML.paths.json", "w") as f:
  json.dump({ k: dict(attrs=list(paths[k]['attrs']), 
                      text=paths[k]['text'],
                      plular=paths[k]['plular']) for k in paths}, f, indent=2)

meta = {}
for k in paths:
  meta[k] = dict(attrs=list(paths[k]['attrs']), text=paths[k]['text'])
  if len(meta[k]['attrs']) == 0: del meta[k]['attrs']
  if not meta[k]['text']: del meta[k]['text']

def nest(flat, sep="/"):
  nested = []

  splited = [p.split(sep) for p in flat.keys()]
  roots = set([P[0] if len(P) > 1 else P[-1] for P in splited])
  for root in roots:
    within = { k[len(root + sep):]: v 
               for k, v in flat.items() 
               if k.startswith(root + sep)}
    R = dict(name=root)
    if len(within) > 0:
      R['nodes'] = nest(within, sep)
    if root in flat:
      if 'attrs' in flat[root]:
        R['attrs'] = flat[root]['attrs']
      if 'text' in flat[root]:
        R['text'] = flat[root]['text']
      if 'plular' in flat[root]:
        R['plular'] = flat[root]['plular']
        
    nested.append(R)

  return nested

meta = nest(meta)

import json
with open("XML.profile.json", "w") as f:
  json.dump(meta, f, indent=2)