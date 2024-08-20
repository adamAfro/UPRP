for p in ["grid.jl", "../paths.jl"] include(p) end
using Revise, CSV, DataFrames, JLD2, ProgressMeter
using Base.Filesystem: joinpath
import .Paths, .Grid

t = Dict( "file" => String,
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
          "px0" => Float64, 
          "px1" => Float64, 
          "px2" => Float64, 
          "px3" => Float64,
          "py0" => Float64, 
          "py1" => Float64, 
          "py2" => Float64, 
          "py3" => Float64,
          "type" => String, 
          "group" => Int )

df = vcat([CSV.read(x, DataFrame, types=t) 
  for x in Paths.Recog.list(labeled=true)]...)
df = filter(row -> row.type in ["docs", "category", "claims"], df)
df[!, :enctype] = Grid.encode.(df.type)
df[!, :id] = 1:size(df, 1)

gdf = groupby(df, [:file, :page])
uniqtp = unique(df.enctype)
uniqgr = [x for x in 1:24]

res = 128
X = zeros(Int, res, res, length(uniqtp), length(gdf))
Y = zeros(Int, res, res, length(uniqgr), length(gdf))

@showprogress 1 for (i, page) in enumerate(gdf)
  
  tgrid, ggrid = Grid.render(page, res;
    from=[:enctype, :group], 
    colsuniqs=[uniqtp, uniqgr])

  X[:, :, :, i] = tgrid
  Y[:, :, :, i] = ggrid
  end

@save joinpath(@__DIR__, "train/grid.X.jld2") X
@save joinpath(@__DIR__, "train/grid.Y.jld2") Y