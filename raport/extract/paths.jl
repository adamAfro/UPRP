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
  const dir = joinpath(@__DIR__, "../docs")
  const dir_labelled = joinpath(@__DIR__, "../labels/output-subset-docs/")

  function get(fid::String; labeled::Bool=false)
    if labeled
      return joinpath(dir_labelled, "$(get_num(fid)).csv")
    else
      return joinpath(dir, "$(get_num(fid)).pdf")
      end
    end

  function list(;labeled::Bool=false)
    if labeled
      return [joinpath(dir_labelled, x) for x in readdir(dir_labelled)]
    else
      return [joinpath(dir, x) for x in readdir(dir)]
      end
    end
  end

    """Ścieżki plików z wynikami rozpoznawania tekstu"""
  module Recog
export get, list
const dir = joinpath(@__DIR__, "../recog/output")
const dir_labelled = joinpath(@__DIR__, "../labels/output")

function get(fid::String; labeled::Bool=false)
  if labeled
    return joinpath(dir_labelled, "$(get_num(fid)).csv")
  else
    return joinpath(dir, "$(get_num(fid)).pdf")
    end
  end

function list(;labeled::Bool=false)
  if labeled
    return [joinpath(dir_labelled, x) for x in readdir(dir_labelled)]
  else
    return [joinpath(dir, x) for x in readdir(dir)]
    end
  end
end
  end