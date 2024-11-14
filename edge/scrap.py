import os
from parse import wrap_cols

# Wczytanie patentów
from pandas import read_csv, DataFrame
patents = read_csv("data/patents.csv")
patents.columns = wrap_cols(patents.columns)
patents = patents.dropna(axis=1, thresh=2)

# Odfiltrowanie patentów z dokumentami
docs = patents.filter(like="other-documents").set_index(patents["id"]).dropna()
docs.columns = [col[-3] if isinstance(col, tuple) else col for col in docs.columns]

# Przetworzenie patentów na zbiór powiązanych dokumentów
import ast
for col in docs.columns:
    if not all(isinstance(x, list) for x in docs[col]):
        docs[col] = docs[col].apply(lambda x: ast.literal_eval(x) if not isinstance(x, list) else x)

# Przetworzenie patentów na zbiór powiązanych dokumentów RAPORT
docs = DataFrame({
    "code": docs["document-code"].explode(),
    "uri": docs["document-uri"].explode()
}, index=docs["document-code"].explode().index)

# Wybór linków
links = docs[(docs['code'] == 'RAPORT') | (docs['code'] == 'RAPORT1')]['uri']

# Pobiernie raportów
from fetch import parallel
from aiohttp import ClientSession
async def download(session: ClientSession, pair:tuple):
    id, url = pair
    async with session.get(url) as response:
        if response.status != 200: return
            
        with open(os.path.join('data', 'RAPORT', id + '.pdf'), 'wb') as f:
            while True:
                chunk = await response.content.read(1024)
                if not chunk: break
                f.write(chunk)
        
        
async def main():
    os.makedirs(os.path.join('data', 'RAPORT'), exist_ok=True)
    await parallel(list(links.items()), download, desc='Pobieranie raportów', limit=10)

import asyncio
asyncio.run(main())