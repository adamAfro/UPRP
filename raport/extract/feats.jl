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
                ptoplft=Pt[], 
                ptoprgt=Pt[], 
                pbtmlft=Pt[], 
                pbtmrgt=Pt[],
                length=Int[],
                thdockeys=Int[],  #table-header-docs
                thcatkeys=Int[],  #table-header-category
                thclmkeys=Int[],  #table-header-claims
                btmkeys=Int[],    #bottom-line
                abbrs=Int[],      #skróty
                strtabbr=Bool[],  #początek
                solwords=Int[],   #pojedyncze
                sentences=Int[],
                shtcodes=Int[],   #krótkie
                lngcodes=Int[])   #długie
fmap = Dict{Tuple{String, Int}, Int}()
  for row in eachrow(df) 
unit = (row.file, row.page)
fmap[unit] = get(fmap, unit, length(fmap) + 1)

pts = [(row.px0, row.py0),
        (row.px1, row.py1),
        (row.px2, row.py2),
        (row.px3, row.py3)]
sort!(pts, by=pt->pt[2])
top, btm = pts[1:2], pts[3:4]
sort!(pts, by=pt->pt[1])
lft, rgt = pts[1:2], pts[3:4]

sentences = Text.wordchain(row.text) |> Text.sententify
  push!(fdf, (fmap[unit],
row.group, encode(row.type),
commonpt(top, lft),
commonpt(top, rgt),
commonpt(btm, lft),
commonpt(btm, rgt),
length(row.text),
intol(row.text, thdockeys, 0.2),
intol(row.text, thcatkeys, 0.2),
intol(row.text, thclmkeys, 0.1),
intol(row.text, btmkeys, 0.1),
any(uppercase(x) == x && length(x) < 4 for x in sentences),
uppercase(sentences[1]) == sentences[1] && length(sentences[1]) < 4,
count(x -> all(isletter, x) && !(' ' in x), sentences),
count(x -> ' ' in x, sentences),
count(x -> all(isdigit, x) && length(x) < 4, sentences),
count(x -> all(isdigit, x) && length(x) >= 4, sentences)
))end#for
return fdf
end#extract
  function commonpt(A, B)::Pt
for a in A
  for b in B
    if a[1] == b[1] && a[2] == b[2] return a end
    end
  end
  return A[1]
  end

    module Text
export wordchain, sententify
  """Zwraca listę słów z tekstu oraz znaków które je oddzielają"""
function wordchain(string::String)
chained = split(string, r"((?=[^a-zA-Z0-9\p{L}])|(?<=[^a-zA-Z0-9\p{L}]))")
chained = filter(x -> !isempty(x), chained)
return chained
end
  """Łączy słowa złożone z liter w sentencje"""
function sententify(strings::Union{Vector{String}, Vector{SubString{String}}})
sentified::Vector{String} = [strings[1]]
lttrprv = all(isletter, strings[1])
prvsent = lttrprv
for string in strings[2:end]
  if all(isletter, collect(string))
    lttrprv = true
    if prvsent
      sentified[end] = sentified[end] * " " * string
    else
      push!(sentified, string)
      prvsent = true
      end
  elseif lttrprv && (all(isspace, collect(string)) || string == ".")
    sentified[end] = sentified[end] * string
  elseif all(x -> isletter(x) || isdigit(x), collect(string)) 
    push!(sentified, string) 
    prvsent = false
  else prvsent = false end
end#for
return sentified
  end#function
end#TextFeatures
  end#module