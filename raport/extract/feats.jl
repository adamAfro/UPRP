  module Features
export docnum, catnum, clmnum, txtnum, extract, widen, widegroup, group
using DataFrames, ProgressMeter

const RowIdenty = Int

include("../labels/meta.jl") 
import .DatasetMeta

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
  function intol(test::String, queries::Vector{String}, tol::Rational)
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

"Punkt na płaszczyźnie" Pt = Tuple{Float32, Float32}
"Czworokąt na płaszczyźnie" Box = Tuple{Pt, Pt, Pt, Pt}

    """Wyciąga cechy z DataFrame"""
  function extract(df::DataFrame)
fdf = DataFrame(unit=Int[], page=Int[],
                group=Int[], 
                doctype=Bool[],
                cattype=Bool[],
                clmtype=Bool[],
                ptoplft=Pt[], 
                ptoprgt=Pt[], 
                pbtmlft=Pt[], 
                pbtmrgt=Pt[],
                length=Int[],
                thdockeys=Int[],  #table-header-docs
                thcatkeys=Int[],  #table-header-category
                thclmkeys=Int[],  #table-header-claims
                # btmkeys=Int[],    #bottom-line
                abbrs=Int[],      #skróty
                strtabbr=Bool[],  #początek
                solwords=Int[],   #pojedyncze
                sentences=Int[],
                shtcodes=Int[],   #krótkie
                lngcodes=Int[])   #długie
fmap = Dict{Tuple{String, Int}, Int}()
  @showprogress 1 "🔎" for row in eachrow(df) 
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
type = encode(row.type)

  push!(fdf, (fmap[unit], row.page,
row.group, 
type == docnum,
type == catnum,
type == clmnum,
commonpt(top, lft),
commonpt(top, rgt),
commonpt(btm, lft),
commonpt(btm, rgt),
length(row.text),
intol(row.text, thdockeys, 2//10),
intol(row.text, thcatkeys, 2//10),
intol(row.text, thclmkeys, 1//10),
# intol(row.text, btmkeys, 1//10),
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




      """Łączy pobliskie czworokąty tworząc szeroką ramkę"""
    function widen(df::Union{DataFrame, SubDataFrame}, n::Int)::Tuple{DataFrame, Matrix{Bool}, Matrix{RowIdenty}}
  labs = zeros(Bool, nrow(df), n)
  ids = zeros(Int, nrow(df), n)
  wide, dfnames = copy(df), filter(x -> !(x in ["unit",
                                                "id",
                                                "group",
                                                # "type",
                                                "ptoplft", 
                                                "ptoprgt", 
                                                "pbtmlft",
                                                "pbtmrgt"]), names(df))
  dcol = Vector{Union{Missing, Float32}}(missing, nrow(df))
  for i in 1:n
for name in dfnames
  wide[!, Symbol("$(name)$(i)")] = Vector{Union{Missing, typeof(df[!, name][1])}}(missing, nrow(df))
  end#for name
wide[!, "xdist$(i)"] = copy(dcol)
wide[!, "ydist$(i)"] = copy(dcol)
  end#for i

  @showprogress 1 "🔍" for (iA, A) in enumerate(eachrow(df))
  dfA = df[Not(iA), :]
  xdists, ydists, sqdists = [], [], []
for r in eachrow(dfA)
push!(xdists, xdist(A, r))
push!(ydists, ydist(A, r))
push!(sqdists, sqrt(xdists[end]^2 + ydists[end]^2))
end#for ir
    nearby = dfA[sortperm(sqdists)[1:min(n, length(sqdists))], :]
  for (i, r) in enumerate(eachrow(nearby))
if hasproperty(r, :id) ids[iA, i] = r.id end
if (A.doctype || A.cattype || A.clmtype) && A.group == r.group
  labs[iA, i] = 1
  end
for name in dfnames
  wide[iA, Symbol("$(name)$(i)")] = r[name]
  end#for name
wide[iA, "xdist$(i)"] = xdists[i]
wide[iA, "ydist$(i)"] = ydists[i]
  end#for i

for i in length(sqdists)+1:n
  wide[iA, "xdist$(i)"], wide[iA, "ydist$(i)"] = (1,1)
  for name in dfnames
    wide[iA, Symbol("$(name)$(i)")] = 0
    end#for name
  end#for i

end#for iA

for name in ["btmlft", "btmrgt", "toplft", "toprgt"]
  wide[!, "x$(name)"] = [pt[1] for pt in wide[!, "p$(name)"]]
  wide[!, "y$(name)"] = [pt[2] for pt in wide[!, "p$(name)"]]
  end; select!(wide, Not(:pbtmlft, :pbtmrgt, :ptoplft, :ptoprgt))

return wide, labs, ids
end#function



      const Grouped = Vector{Vector{Int}}

    """Szuka grup na podstawie macierzy połączeń sąsiedztwa"""
  function widegroup(I::Matrix{RowIdenty}, B::Matrix{Bool})::Grouped
@assert size(I) == size(B) "I, B muszą być tej samej wielkości"
@assert all(row -> all(x -> x == 0 || x in 1:size(I, 1), row), eachrow(I)) """
  Indeksy w I muszą zgadzać się z numeracją wierszy macierzy I,B / być 0
"""
ptrs, grps = Dict{Int, Int}(), Vector{Set{Int}}()
for (row, belongings) in enumerate(eachrow(B))
  ids = I[row, findall(belongings)]

  trgtG = 0
  # przypisuje istniejącej grupy
  if haskey(ptrs, row) trgtG = ptrs[row]
  else for id in ids
    if haskey(ptrs, id) trgtG = ptrs[id]; break end
    end#for
  end#if

  if trgtG == 0 # tworzy nową grupę
    push!(grps, Set([row])) # z tym indeksem
    trgtG = length(grps) # zamienia docelową
    end

  for id in ids
    if haskey(ptrs, id) # id ma już przypisaną grupę
      prvG = ptrs[id] # poprzednia grupa
      if prvG == trgtG continue end
      for idtaken in grps[prvG] ptrs[idtaken] = trgtG end
      empty!(grps[prvG]) # zamiast usuwania starej
      # usuwanie spowodowałoby niekontrolowaną zmianę indeksów
    else
      ptrs[id] = trgtG # przypisanie nowej grupy
      push!(grps[trgtG], id) # dodanie do grupy
      end
    end#for
  end#for row
groups = [sort(Vector([x for x in G])) for G in grps if !isempty(G)]
return sort(groups, by=g -> g[1])
end#group




    """Szuka grup na podstawie zmiennej"""
  function group(df::DataFrame, unit::Union{Symbol, Vector{Symbol}})::Grouped
@assert all(x -> String(x) in names(df), unit) """
  Kolumny podziału jednostek muszą być
"""
@assert "group" in names(df) "Musi być kolumna grupy"
@assert "id" in names(df) "Musi być kolumna id"
groups = []
for unitdf in groupby(df, unit)
  for gdf in groupby(unitdf, :group)
    push!(groups, gdf.id)
    end#for
  end#for
return sort(groups, by=g -> g[1])
end#group





    """Oblicza najmniejszą odległość między dwoma prostokątami(!)"""
  function xdist(A::DataFrameRow, B::DataFrameRow)::Float32
d, inter = 0.0, max(0, min(A.ptoprgt[1], B.ptoprgt[1]) 
              - max(A.ptoplft[1], B.ptoplft[1]))
  if inter == 0
leftsideA = A.ptoplft[1] < B.ptoplft[1]
if leftsideA d = B.ptoplft[1] - A.ptoprgt[1]
        else d = B.ptoprgt[1] - A.ptoplft[1] end
        end#if
        return d
        end#function




    """Oblicza najmniejszą odległość między dwoma prostokątami(!)"""
  function ydist(A::DataFrameRow, B::DataFrameRow)::Float32
d, inter = 0.0, max(0, min(A.pbtmrgt[2], B.pbtmrgt[2]) 
              - max(A.ptoprgt[2], B.ptoprgt[2]))
  if inter == 0
ontopA = A.ptoplft[2] < B.ptoplft[2]
if ontopA d = B.ptoplft[2] - A.pbtmlft[2]
     else d = B.pbtmlft[2] - A.ptoplft[2] end
     end#if
     return d
     end#function




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