module Spatial



const Number = Float32
const Point = Tuple{Number, Number}
const Quadrangle = Tuple{Point, Point, Point, Point}
const Segment = Tuple{Point, Point}



"""Rzut na oś-x odległości między prawą i lewą stroną 2 czworokątów,
albo 0 jeśli rzut krawędzi się przecina, zakłada posortowanie min x[i] < min x[j]"""
function mxoffset(lfts::Vector{Number}, rgts::Vector{Number}, pairs::Vector{CartesianIndex{2}})::Matrix{Number}
@assert length(lfts) == length(rgts) "Długości wektorów muszą być równe"
D = Matrix( fill(Inf, length(lfts), length(rgts)) )
for idx in pairs i, j = Tuple(idx)
  if inside(lfts[i], (lfts[j], rgts[j])) ||
     inside(rgts[i], (lfts[j], rgts[j])) D[i, j] = 0; continue end
  if inside(lfts[j], (lfts[i], rgts[i])) ||
     inside(rgts[j], (lfts[i], rgts[i])) D[i, j] = 0; continue end
  if lfts[i] < lfts[j]
    D[i, j] = +(lfts[j] - rgts[i])
    D[j, i] = -(lfts[j] - rgts[i])
  else
    D[i, j] = -(lfts[i] - rgts[j])
    D[j, i] = +(lfts[i] - rgts[j])
  end
end
return D
end



"Rzuty na oś y odległości między środkami par czworokątów"
function mxshift(middles::Vector{Point}, pairs::Vector{CartesianIndex{2}})::Matrix{Number}
H = Matrix( fill(Inf, length(middles), length(middles)) )
for idx in pairs i, j = Tuple(idx)
  H[i, j] = +(middles[j][2] - middles[i][2])
  H[j, i] = -(middles[i][2] - middles[j][2])
end
return H
end



function inside(x::Number, range::Tuple{Number, Number})::Bool
return (x >= range[1] && x <= range[2])   
end



"Tworzy macierz przejść między środkami czworokątów nie przecinających czworokątów"
function mxpassage(Qs::Vector{Quadrangle}, middles::Vector{Point})::Matrix{Bool}
n = length(Qs)
B = Matrix( zeros(Bool, n, n) )
for i in 1:n for itest in (i + 1):n for iobst in 1:n if i != iobst && itest != iobst
  B[i, itest] = Spatial.passage(middles[i], middles[itest], Qs[iobst])
  B[itest, i] = B[i, itest]
  if !B[i, itest] break end
end end end end#for
return B
end#passage



"Sprawdza, czy istnieje przejście między środkami czworokątów nie przecinające czworokąta"
function passage(from::Point, to::Point, through::Quadrangle)::Bool
for edge in [ (through[1], through[2]),
              (through[2], through[3]),
              (through[3], through[4]),
              (through[4], through[1]) ] if crosses((from, to), edge) return false end end
return true
end



"Sprawdza czy 2 odcinki się przecinają"
function crosses(A::Segment, B::Segment)::Bool
if vtest(A)
  if vtest(B) if A[1][1] == B[1][1] 
  return inbetween(A[1][2], B; ax=2) ||
         inbetween(A[2][2], B; ax=2)
  else false end end
  return vcrosses(A, B)
  end
if vtest(B) return vcrosses(B, A) end
eqA, eqB = coef(A[1], A[2]), coef(B[1], B[2])
if eqA[1] == eqB[1] return false end #parallel
x = (eqB[2] - eqA[2]) / (eqA[1] - eqB[1])
return inbetween(x, A; ax=1) && inbetween(x, B; ax=1)
end#crosses



"Sprawdza czy odcinki są pionowe"
function vtest(L::Segment)::Bool
return L[1][1] == L[2][1]
end



"Sprawdza czy 2 odcinki się przecinają, gdy jeden z nich jest pionowy"
function vcrosses(V::Segment, L::Segment)::Bool
x = V[1][1]
if !inbetween(x, L; ax=1) return false end
eq = coef(L[1], L[2])
y = eq[1]*x + eq[2]
return inbetween(y, V; ax=2)
end



"Współczynniki 2-wymiarowej funkcji liniowej"
function coef(A::Point, B::Point)
a = (B[2] - A[2]) / (B[1] - A[1])
return a, A[2] - a * A[1]
end



"Sprawdza czy punkt podanej osi leży w 2-wymiarowym odcinku `L`"
function inbetween(x::Number, L::Segment; ax::Int)::Bool
return x >= minimum([L[1][ax], L[2][ax]]) && x <= maximum([L[1][ax], L[2][ax]])
end



end#module