using Statistics: mean, median
import Flux as Neural; using CUDA, BSON, ProgressMeter, CSV, DataFrames
include("data.jl")
import .Data

💾 = Data.load()

M = Matrix(💾[:, [Data.colnames[:pos]..., Data.colnames[:tbl]..., Data.colnames[:txt]...]])
# fmodel = BSON.load("fmodel.bson")[:fmodel]
pred = fmodel(M' |> Neural.gpu) |> Neural.cpu |> vec
CSV.write("isdata.pred.csv", DataFrame(pred=pred), header=false)