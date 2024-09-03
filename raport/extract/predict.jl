using CSV, DataFrames, ProgressMeter, LinearAlgebra, Statistics, Random
for pth in ["../paths.jl", "edges.jl", "textual.jl"] include(pth) end
import .Edges, .Paths, .Textual
import Flux as Neural; using CUDA, BSON



df = vcat([CSV.read(p, DataFrame; types=Dict(
"file" => String, 
"text" => String, 
"page" => Int,
"x0" => Int, 
"x1" => Int, 
"x2" => Int, 
"x3" => Int,
"y0" => Int, 
"y1" => Int, 
"y2" => Int, 
"y3" => Int,
"px0" => Float32, 
"px1" => Float32, 
"px2" => Float32, 
"px3" => Float32, 
"py0" => Float32, 
"py1" => Float32, 
"py2" => Float32, 
"py3" => Float32
)) for p in Paths.Recog.list()]...)

df.id = 1:nrow(df)
df.unit = zeros(Int, nrow(df))
umap = Dict{Tuple{String, Int}, Int}()
for row in eachrow(df)
       unit = (row.file, row.page)
  umap[unit] = get(umap, unit, length(umap) + 1)
  row[:unit] = umap[unit]
end#progress

txtfeats = [:codes, :years, :sentences, :length, :width, :dockeyword]
sented = Textual.sententify.( Textual.wordchain.(df.text) )
df.codes .= [count(Textual.codealike, S) for S in sented]
df.years .= [count(Textual.yearalike, S) for S in sented]
df.sentences = length.(sented) .- df.codes .- df.years
df.length = length.(df.text)
df.width = df.px2 .- df.px0
df.dockeyword .= [maximum(s -> Textual.dockeyword(s, 0.1), S) for S in sented]

tblfeats = [:updoc, :dwdoc]
df.updoc = zeros(Float32, nrow(df))
df.dwdoc = zeros(Float32, nrow(df))
for unit in groupby(df, :unit)
docs = filter(row -> row.dockeyword > 0, unit)
if isempty(docs) continue end
for quad in eachrow(unit)
if quad.id in docs.id continue end
updiff = filter(x -> x > 0, quad.py0 .- docs.py3)
if !isempty(updiff) quad.updoc = minimum(updiff) end
dwdiff = filter(x -> x > 0, docs.py0 .- quad.py3)
if !isempty(dwdiff) quad.dwdoc = minimum(dwdiff) end
end
end

posfeats = [:px0, :px1, :px2, :px3, :py0, :py1, :py2, :py3]


M = Matrix(df[:, [posfeats..., tblfeats..., txtfeats...]])
# fmodel = BSON.load("fmodel.bson")[:fmodel]
pred = fmodel(M' |> Neural.gpu) |> Neural.cpu |> vec
df = df[round.(Int, pred) .== 1, :]

df.group = 1:nrow(df)
M, E = Edges.mx(df[:, [:unit, :group, posfeats..., txtfeats...]])
M = replace(M, Inf32 => 2.0f0)

# gmodel = BSON.load("gmodel.bson")[:gmodel]
pred = gmodel(M' |> Neural.gpu) |> Neural.cpu |> vec
E.group .= round.(Int, pred)
groups = Edges.group(E)

