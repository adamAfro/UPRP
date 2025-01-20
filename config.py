data = { 'UPRP': 'api.uprp.gov.pl',
         'Lens': 'api.lens.org',
         'USPG': 'developer.uspto.gov/grant',
         'USPA': 'developer.uspto.gov/application',
         'Google': 'patents.google.com' }

class Colr:

  neutral = 'blue'
  good = 'green'
  mid = 'yellow'
  attention = 'orange'
  warning = 'red'
  
class Cmap:

  neutral = 'Blues'
  good = 'Greens'
  mid = 'YlOrBr'
  attention = 'Oranges'
  warning = 'Reds'

import matplotlib.pyplot as plt
plt.rcParams['axes.spines.top'] = False
plt.rcParams['axes.spines.right'] = False