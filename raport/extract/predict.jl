import Flux as Neural; using CUDA, BSON, ProgressMeter
include("data.jl")
import .Data

df = Data.load()

M = Matrix(tr[:, [Data.colnames[:pos]..., Data.colnames[:tbl]..., Data.colnames[:txt]...]])
# fmodel = BSON.load("fmodel.bson")[:fmodel]
pred = fmodel(M' |> Neural.gpu) |> Neural.cpu |> vec
df = df[round.(Int, pred) .== 1, :]

df.group = 1:nrow(df)
M, E = Edges.mx(tr[:, [:unit, :group, Data.colnames[:pos]..., Data.colnames[:txt]...]])
M = replace(M, Inf32 => 2.0f0)

# gmodel = BSON.load("gmodel.bson")[:gmodel]
pred = gmodel(M' |> Neural.gpu) |> Neural.cpu |> vec
E.group .= round.(Int, pred)
groups = Edges.group(E)