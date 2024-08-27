module Dist
  export x, y





    """Oblicza najmniejszą odległość między dwoma prostokątami(!)"""
  function x(A, B)::Float32
@assert hasproperty(A, :xtoplft) && 
        hasproperty(A, :xtoprgt) && 
        hasproperty(A, :xbtmlft) && 
        hasproperty(A, :xbtmrgt) "A musi być prostokątem"
@assert hasproperty(B, :xtoplft) && 
        hasproperty(B, :xtoprgt) && 
        hasproperty(B, :xbtmlft) && 
        hasproperty(B, :xbtmrgt) "A musi być prostokątem"
d, inter = 0.0, max(0, min(A.xtoprgt, B.xtoprgt) 
              - max(A.xtoplft, B.xtoplft))
  if inter == 0
leftsideA = A.xtoplft < B.xtoplft
if leftsideA d = B.xtoplft - A.xtoprgt
        else d = B.xtoprgt - A.xtoplft end
        end#if
        return d
        end#function




    """Oblicza najmniejszą odległość między dwoma prostokątami(!)"""
  function y(A, B)::Float32
@assert hasproperty(A, :ytoplft) && 
        hasproperty(A, :ytoprgt) && 
        hasproperty(A, :ybtmlft) && 
        hasproperty(A, :ybtmrgt) "A musi być prostokątem"
@assert hasproperty(B, :ytoplft) && 
        hasproperty(B, :ytoprgt) && 
        hasproperty(B, :ybtmlft) && 
        hasproperty(B, :ybtmrgt) "A musi być prostokątem"
d, inter = 0.0, max(0, min(A.ybtmrgt, B.ybtmrgt) 
              - max(A.ytoprgt, B.ytoprgt))
  if inter == 0
ontopA = A.ytoplft < B.ytoplft
if ontopA d = B.ytoplft - A.ybtmlft
     else d = B.ybtmlft - A.ytoplft end
     end#if
     return d
     end#function




end#module