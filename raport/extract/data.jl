module Data
using CSV, DataFrames, ProgressMeter, Statistics, Random
for pth in ["../paths.jl", "textual.jl"] include(pth) end
import .Paths, .Textual



colnames = Dict(
:pos => [:x, :y],
:pts => [:px0, :px1, :px2, :px3, :py0, :py1, :py2, :py3],
:txt => [:codes, :years, :sentences, :length, :width, :dockeyword],
:tbl => [:updoc, :dwdoc])



function load(;train=false)::Union{Tuple{DataFrame, DataFrame}, DataFrame}

ps = Paths.Recog.list(labeled=train)
dfs = []
@showprogress "💾" for p in ps
push!(dfs, CSV.read(p, DataFrame; types=Dict(
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
))) end#for
df = vcat(dfs...)

df.x = Float32.((df.x0 .+ df.x1 .+ df.x2 .+ df.x3) ./ 4)
df.y = Float32.((df.y0 .+ df.y1 .+ df.y2 .+ df.y3) ./ 4)

df.id = 1:nrow(df)
df.unit = zeros(Int, nrow(df))
umap = Dict{Tuple{String, Int}, Int}()
for row in eachrow(df)
       unit = (row.file, row.page)
  umap[unit] = get(umap, unit, length(umap) + 1)
  row[:unit] = umap[unit]
end#progress

sented = Textual.sententify.( Textual.wordchain.(df.text) )
df.codes .= [count(Textual.codealike, S) for S in sented]
df.years .= [count(Textual.yearalike, S) for S in sented]
df.sentences = length.(sented) .- df.codes .- df.years
df.length = length.(df.text)
df.width = df.px2 .- df.px0
df.dockeyword .= [maximum(s -> Textual.dockeyword(s, 0.1), S) for S in sented]

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

if !train return df end

splitf = round(Int, 0.8 * (unique(df[:, :unit]) |> length))
df.test = zeros(Bool, nrow(df))
for (i, unit) in enumerate(groupby(df, :unit))
unit[!, :test] .= i > splitf
end
return filter(row -> !row.test, df), filter(row -> row.test, df)
end



end