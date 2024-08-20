module Features
  export docnum, catnum, clmnum, txtnum, intol, extract
  using DataFrames

    "typ etykiety tekstu - referencja dokumentu"      const docnum = 1
    "typ etykiety tekstu - kategoria dokument"        const catnum = 2
    "typ etykiety tekstu - zastrzeżenia do dokumentu" const clmnum = 3
    "typ etykiety tekstu - brak etykiety"             const txtnum = 0

    """Koduje typ tekstu do liczby"""
  function encode(type::String)
    if type == "docs"     return docnum
elseif type == "category" return catnum
elseif type == "claims"   return clmnum
else                      return txtnum end
end


    using StringDistances
  function intol(test::String, queries::Vector{String}, tol::Float64)
"""Sprawdza czy przynajmniej jeden z tekstów 
występuje w tekście z dokładnością do tolerancji"""
for q in queries
  d = evaluate(Levenshtein(), test, q)
  if d/length(q) <= tol
    return true
    end
  end
return false
end


"słowa kluczowe spotykane w nagłówkach tabel nad referencjami"
const thdockeys = ["dokumenty - z podaną identyfikacją"]

"słowa kluczowe spotykane w nagłówkach tabel nad kategoriami"
const thcatkeys = ["kategoria dokumentu", "kategoria", "dokumentu"]

"słowa kluczowe spotykane w nagłówkach tabel nad zastrzeżeniami"
const thclmkeys = ["odniesienie do zastrzeżeń", 
  "odniesienie",  "zastrzeżeń", "zastrzezenia", 
  "do zastrz", "nr"]

"słowa kluczowe spotykane w dolnej linii tabeli"
const btmkeys = ["strona x z x",
  "dalszy ciąg wykazu dokumentów na następnej stronie",
  "dokument wskazany przez głaszającego",
  "dokument podważający nowość wynalazku",
  "dokument podważający nowość rozwiązania",
  "dokument określający ogólny stan techniki",
  "sprawozdanie wykonał"]

"Punkt na płaszczyźnie" Pt = Tuple{Float64, Float64}
"Czworokąt na płaszczyźnie" Box = Tuple{Pt, Pt, Pt, Pt}

    """Wyciąga cechy z DataFrame"""
  function extract(df::DataFrame)
fdf = DataFrame(unit=Int[], 
                group=Int[], 
                type=Int[],
                pt0=Pt[], 
                pt1=Pt[], 
                pt2=Pt[], 
                pt3=Pt[],
                length=Int[], 
                lowercase=Int[], 
                uppercase=Int[], 
                digit=Int[],
                #keywords
                thdockeys=Int[],  #table-header-docs
                thcatkeys=Int[],  #table-header-category
                thclmkeys=Int[],  #table-header-claims
                btmkeys=Int[])    #bottom-line
fmap = Dict{Tuple{String, Int}, Int}()
for row in eachrow(df) 

  unit = (row.file, row.page)
  fmap[unit] = get(fmap, unit, length(fmap) + 1)
  
  push!(fdf, (fmap[unit],
    row.group, encode(row.type),
    (row.px0, row.py0), 
    (row.px1, row.py1),
    (row.px2, row.py2), 
    (row.px3, row.py3),
    length(row.text), 
    count(islowercase, row.text),
    count(isuppercase, row.text),
    count(isdigit, row.text),
    intol(row.text, thdockeys, 0.2),
    intol(row.text, thcatkeys, 0.2),
    intol(row.text, thclmkeys, 0.1),
    intol(row.text, btmkeys, 0.1)))
  end
return fdf
end
  end#module