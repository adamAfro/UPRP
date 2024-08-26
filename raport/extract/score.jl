module Score
  using ProgressMeter, DataFrames




  function mxconfussion(pred, labs, tres = 0.5)
mxnrow, mxncol = size(labs)
C = DataFrame(d=Int[], obs=Int[], value=Symbol[])
for c in 1:mxncol for r in 1:mxnrow
  predtrue = pred[r, c] > tres
  labtrue = labs[r, c] > 0.5
  if predtrue && labtrue
    push!(C, (r, c, :TP))
  elseif predtrue == true
    push!(C, (r, c, :FP))
  elseif labtrue == true
    push!(C, (r, c, :FN))
  else
    push!(C, (r, c, :TN))
    end#if
  end#row
  end#col
return C
end





    """Dosyć kosztowna funkcja szukająca dobranej grupy i oceniająca"""
  function mxcount(Gpred::Vector{Vector{Int}}, G::Vector{Vector{Int}})
sort!(Gpred, by=length, rev=true)
sort!(G,     by=length, rev=true)
stats = DataFrame(n=fill(Int(0), length(G)),
                  max=map(length, G),
                  groups=fill(Int(0), length(G)))

gmth, pmth = [], []
@showprogress 1 "💏" for (gid, g) in enumerate(G)
  bestp, bestn = 0, 0
  for (pid, p) in enumerate(Gpred)
    n = length(intersect(p, g))
    if n > bestn 
      bestp = pid
      bestn = n 
      end
    end#for
  if bestn > 0
    push!(gmth, gid)
    push!(pmth, bestp)
    stats[gid, :n] = bestn
    stats[gid, :max] = length(g)
    for (ogid, other) in enumerate(G)
      stats[gid, :groups] += length(intersect(g, other)) > 0
      end#for
    end
  end#for g

added = DataFrame(n=Int[], groups=Int[])
for pid in setdiff(1:length(Gpred), pmth)
  groups = 0
  for g in G
    groups += length(intersect(Gpred[pid], g)) > 0
    end#for g
  push!(added, (length(Gpred[pid]), groups))
  end#for p

omitted = DataFrame(gid=Int[], n=Int[], groups=Int[])
for gid in setdiff(1:length(G), gmth)
  groups = 0
  for p in Gpred
    groups += length(intersect(p, G[gid])) > 0
    end#for p
  push!(omitted, (gid, length(G[gid]), groups))
  end#for g
return stats, added, omitted
end#gstat





end#module