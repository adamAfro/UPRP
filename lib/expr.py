import re

def parse(exprdict:dict[str, str]):
  return re.compile('(?:' + '|'.join(rf'(?P<{k}>{q})' 
    for k, q in exprdict.items()) + ')')

class Marker:

  def __init__(self, Q:dict, K0:list=[]): 
    self.Q = parse(Q)
    self.K0 = K0

  def union(self, x:str):
    I = re.finditer(self.Q, x)
    S = [(m.group(), m.start(), m.end(), m.groupdict()) for m in I]
    if not S: return []
    S = [(x, a, b, any(m[k] is not None for k in self.K0)) for x, a, b, m in S]
    S.sort(key=lambda x: x[1])
    M = [(S[0])]
    for x, a, b, m in S[1:]:
      x0, a0, b0, m0 = M[-1]
      if a > b0: M.append((x, a, b, m))
      else:
        M[-1] = (x0 + x, a0, max(b0, b), m0 or m)
    return M