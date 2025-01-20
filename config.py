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

import matplotlib.pyplot as plt
plt.rcParams['axes.spines.top'] = False
plt.rcParams['axes.spines.right'] = False