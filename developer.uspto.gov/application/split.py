import os

k = 'ipa241212.xml'
o = 'raw'

with open(k, 'r', encoding='utf-8') as f:
  X = f.read()

S = X.split('<?xml')
S.pop(0)

for i, y in enumerate(S):
  y = '<?xml' + y
  O = os.path.join(o, f'a{i+1}.xml')
  with open(O, 'w', encoding='utf-8') as f:
    f.write(y)