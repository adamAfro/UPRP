using Statistics: mean, median
import Flux as Neural; using CUDA, BSON, ProgressMeter, CSV, DataFrames
include("data.jl")
import .Data

💾 = Data.load()

M = Matrix(💾[:, [Data.colnames[:pos]..., Data.colnames[:tbl]..., Data.colnames[:txt]...]])
# fmodel = BSON.load("fmodel.bson")[:fmodel]
pred = fmodel(M' |> Neural.gpu) |> Neural.cpu |> vec
💾 = 💾[round.(Int, pred) .== 1, :]

💾.id = 1:nrow(💾)
💾.group = 1:nrow(💾)
ME, E = Edges.mx(💾[:, [:unit, :group, Data.colnames[:pts]..., Data.colnames[:txt]...]])
ME = replace(ME, Inf32 => 2.0f0)

# gmodel = BSON.load("gmodel.bson")[:gmodel]
pred = gmodel(ME' |> Neural.gpu) |> Neural.cpu |> vec
E.group .= round.(Int, pred)
groups = Edges.group(E)

ig = maximum(💾.group) + 1
@showprogress for g in groups
💾[g, :group] .= ig; ig += 1
end

🖇 = DataFrame( P=Union{Int,Missing}[],
                category=Union{String,Missing}[], 
                docs=Union{String,Missing}[],
                claims=Union{String,Missing}[])

💾.lft .= min.(💾.px0, 💾.px3)
💾.medlft .= fill(missing, nrow(💾)) |> Vector{Union{Missing, Float32}}
@showprogress "📏" for 📄 in groupby(💾, :unit)
📄.medlft .= median(📄.lft)
end

@showprogress "🖇" for 🔗 in groupby(💾, :group)
  push!(🖇, ( P=missing, 
              category=missing, 
              docs=missing, 
              claims=missing ))

  lftside = 🔗.lft .< 🔗.medlft
  short = length.(🔗.text) .< 3
  🔗cat = 🔗[lftside .&& short, :]
  if !isempty(🔗cat) 🖇[end, :category] = "" end
  for text in 🔗cat[:, :text] 🖇[end, :category] *= text end

  rgt = min.(🔗.px1, 🔗.px2)
  rgtside = rgt .> median(rgt)
  ndigits = [count(isdigit, text) for text in 🔗.text]
  nletter = [count(isletter, text) for text in 🔗.text]
  numeric = ndigits .> length.(🔗.text)/2 .&& nletter .< 2
  clmkeyword = "zastrz" ∈ lowercase.(🔗.text)
  🔗clm = 🔗[rgtside .&& (numeric .|| clmkeyword), :]
  if !isempty(🔗clm) 🖇[end, :claims] = "" end
  for text in 🔗clm[:, :text] 🖇[end, :claims] *= text end

  🔗doc = 🔗[.!(lftside .&& short) .&& .!(rgtside .&& (numeric .|| clmkeyword)), :]
  if !isempty(🔗doc) 🖇[end, :docs] = "" end
  for text in 🔗doc[:, :text] 🖇[end, :docs] *= text end
  
end