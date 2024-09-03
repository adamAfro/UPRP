module Paths
export PDF, OCR
using Base.Filesystem: joinpath



function get_num(fid::String)
  num = fid
  num = occursin("/", num) ? num = split(num, "/")[end] : num
  num = occursin(".", num) ? split(num, ".")[1] : num
  return num
end



"""Ścieżki oryginalnych dokumentów pobranych z UPRP.API"""
module Docs
export get, list
const alldir = joinpath(@__DIR__, "docs")



function get(fid::String)
  num = get_num(fid)
  folders = readdir(alldir)
  matches = [match(r"(\d+)-(\d+)", p) for p in folders]
  rnumbers = [(m.captures[1], m.captures[2]) for m in matches]
  ranges = [(parse(Int, x[1]), parse(Int, x[2])) for x in rnumbers]
  for (i, (start, stop)) in enumerate(ranges)
    if start <= parse(Int, num) <= stop
      return joinpath(alldir, folders[i], fid)
    end
  end
end

function list(; labeled::Bool=false)
  throw(ArgumentError("Not implemented"))
end
end#module



"""Ścieżki plików z wynikami rozpoznawania tekstu"""
module Recog
export get, list
const alldir = joinpath(@__DIR__, "recog/output")
const labdir = joinpath(@__DIR__, "labels/output")

function get(fid::String; labeled::Bool=false)
  dir = labeled ? labdir : alldir
  num = get_num(fid)
  folders = readdir(dir)
  matches = [match(r"(\d+)-(\d+)", p) for p in folders]
  rnumbers = [(m.captures[1], m.captures[2]) for m in matches]
  ranges = [(parse(Int, x[1]), parse(Int, x[2])) for x in rnumbers]
  for (i, (start, stop)) in enumerate(ranges)
    if start <= parse(Int, num) <= stop
      return joinpath(dir, folders[i], fid)
    end
  end
end

function list(; labeled::Bool=false)
  if labeled
    return [joinpath(labdir, x) for x in readdir(labdir)]
    end

  folders = readdir(alldir)
  files = [readdir(joinpath(alldir, f)) for f in folders]
  paths = []
  for (i, fs) in enumerate(files)
    for f in fs
      push!(paths, joinpath(alldir, folders[i], f))
    end
  end
  return paths
end
end#module



end