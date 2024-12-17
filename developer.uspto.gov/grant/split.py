import os

k = 'ipg241217.xml'
o = 'raw'

with open(k, 'r', encoding='utf-8') as f:
  X = f.read()

S = X.split('<?xml')
S.pop(0)

for i, y in enumerate(S):
  y = '<?xml' + y
  O = os.path.join(o, f'g{i+1}.xml')
  with open(O, 'w', encoding='utf-8') as f:
    f.write(y)