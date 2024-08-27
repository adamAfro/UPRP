  module Features
export docnum, catnum, clmnum, txtnum, extract, widen, widegroup, group
using DataFrames, ProgressMeter, Random, Statistics

const RowIdenty = Int

for p in ["text.jl", "dist.jl", "../labels/meta.jl"]
  include(p)
  end
import .DatasetMeta, .Text, .Dist

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
                xtoplft=Float32[],
                ytoplft=Float32[],
                xtoprgt=Float32[],
                ytoprgt=Float32[],
                xbtmlft=Float32[],
                ybtmlft=Float32[],
                xbtmrgt=Float32[],
                ybtmrgt=Float32[],
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
commonpt(top, lft)[1],
commonpt(top, lft)[2],
commonpt(top, rgt)[1],
commonpt(top, rgt)[2],
commonpt(btm, lft)[1],
commonpt(btm, lft)[2],
commonpt(btm, rgt)[1],
commonpt(btm, rgt)[2],
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



        const WideSet = Tuple{DataFrame, Matrix{Bool}, Matrix{RowIdenty}}
      """Łączy pobliskie czworokąty tworząc szeroką ramkę"""
    function widen(df::Union{DataFrame, SubDataFrame}, n::Int)::WideSet
  labs = zeros(Bool, nrow(df), n)
  ids = zeros(Int, nrow(df), n)
  wide, dfnames = copy(df), filter(x -> !(x in ["unit",
                                                "id",
                                                "group",
                                                # "type",
                                                "xtoplft", "ytoplft", 
                                                "xtoprgt", "ytoprgt", 
                                                "xbtmlft", "ybtmlft",
                                                "xbtmrgt", "ybtmrgt"]), names(df))
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
push!(xdists, Dist.x(A, r))
push!(ydists, Dist.y(A, r))
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




    """Tworzy wektor grupujący dopasowany do jednostek badawczych"""
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





struct Augment
  n::Int
  xnoise::Float32
  ynoise::Float32
  lnoise::Float32
  abbr::Bool
  solwords::Bool
  sentences::Bool
  shtcodes::Bool
  lngcodes::Bool
  end
function Augment()
  return Augment(0, 0, 0, 0, false, false, false, false, false)
  end


      const WideMxSet = Tuple{
        Matrix{Float32}, Matrix{Bool}, Matrix{RowIdenty}, Grouped,
        Matrix{Float32}, Matrix{Bool}, Matrix{RowIdenty}, Grouped
      }
    """Łączy w jedną, szeroką ramkę, stosuje prepocessing i 
    zwraca obiekty matematyczne podzielone na zbiór treningowy i walidacyjny"""
  function widenmx(df::DataFrame, NNeight::Int; trsplit::Rational=4//5, aug::Augment=Augment())::WideMxSet
filter!(row -> row.type != "text", df)
gFE = groupby(Features.extract(df), :unit)
gFE = gFE[shuffle(1:size(gFE, 1))]
gFE = collect(gFE)

split = round(Int, trsplit*length(gFE))
  
  for i in 1:aug.n for unit in gFE[1:split]
avgh = mean(unit[!, :ybtmlft] .- unit[!, :ytoplft])
Aug = copy(unit)

Aug[:, :ytoplft] .+= aug.xnoise*(rand(nrow(Aug)) .- .5)*avgh
Aug[:, :ytoprgt] .+= aug.xnoise*(rand(nrow(Aug)) .- .5)*avgh
Aug[:, :ybtmlft] .+= aug.xnoise*(rand(nrow(Aug)) .- .5)*avgh
Aug[:, :ybtmrgt] .+= aug.xnoise*(rand(nrow(Aug)) .- .5)*avgh

Aug[:, :xtoplft] .+= aug.ynoise*(rand(nrow(Aug)) .- .5)*avgh
Aug[:, :xtoprgt] .+= aug.ynoise*(rand(nrow(Aug)) .- .5)*avgh
Aug[:, :xbtmlft] .+= aug.ynoise*(rand(nrow(Aug)) .- .5)*avgh
Aug[:, :xbtmrgt] .+= aug.ynoise*(rand(nrow(Aug)) .- .5)*avgh

if aug.lnoise > 0
  addit = aug.lnoise .* (rand(nrow(Aug)) .- .5) .* (Aug[:, :length] .> 8)
  Aug[:, :length] .+= ceil.(addit)
  end

if aug.abbr
  Aug[:, :abbrs] .+= rand(0:2, nrow(Aug)) .* (Aug[:, :abbrs] .> 1)
  end

if aug.solwords
  Aug[:, :solwords] .+= rand(-1:2, nrow(Aug)) .* (Aug[:, :solwords] .> 2)
  end

if aug.sentences
  Aug[:, :sentences] .+= rand(-1:2, nrow(Aug)) .* (Aug[:, :sentences] .> 2)
  end  

if aug.shtcodes
  Aug[:, :shtcodes] .+= rand(-1:2, nrow(Aug)) .* (Aug[:, :shtcodes] .> 2)
  end

if aug.lngcodes
  Aug[:, :lngcodes] .+= rand(-1:2, nrow(Aug)) .* (Aug[:, :lngcodes] .> 2)
  end  

insert!(gFE, split+1, Aug)
end
  end#Aug

split *= 1+aug.n



idtr = 1; for unit in gFE[1:split]
  unit[!, :id] = idtr:idtr+nrow(unit)-1
  idtr += nrow(unit)
  end#idtr
idvl = 1; for unit in gFE[split+1:end]
  unit[!, :id] = idvl:idvl+nrow(unit)-1
  idvl += nrow(unit)
  end#idvl


Uwides = Vector{DataFrame}()
Ulabs = Vector{Matrix}()
Uids = Vector{Matrix}()
@showprogress 1 "🪗 " for unit in gFE
  wide, labs, ids = Features.widen(unit, NNeight)
  push!(Uwides, wide) 
  push!(Ulabs, labs)
  push!(Uids, ids)
  end

Wtr = vcat(Uwides[1:split]...)
ytr = vcat(Ulabs[1:split]...)
Itr = vcat(Uids[1:split]...)
Gtr = Features.group(Wtr, [:unit])

Wvl = vcat(Uwides[split+1:end]...)
yvl = vcat(Ulabs[split+1:end]...)
Ivl = vcat(Uids[split+1:end]...)
Gvl = Features.group(Wvl, [:unit])

Mtr = Matrix{Float32}(select(Wtr, Not([:id, :group, :unit])))
Mvl = Matrix{Float32}(select(Wvl, Not([:id, :group, :unit])))
for col in 1:size(Mtr, 2)
  if all(v -> v == 0 || v == 1, Mtr[:, col]) m, s = 0, 1
  else m, s = mean(Mtr[:, col]), std(Mtr[:, col]) end
  if s == 0 s = 1 end
  Mtr[:, col] = (Mtr[:, col] .- m) ./ s
  Mvl[:, col] = (Mvl[:, col] .- m) ./ s
  end#for col

return Mtr, ytr, Itr, Gtr, Mvl, yvl, Ivl, Gvl
end#dataload
  end#module