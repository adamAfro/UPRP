cd "$(dirname "$0")"

rm -rf workdir/*
if [ -z "$1" ]; then

pdflatex -output-directory=workdir main.tex
cd workdir
bibtex main
cd ../
pdflatex -output-directory=workdir main.tex #tak trzeba niby
pdflatex -output-directory=workdir main.tex #z jakiegoś powodu

current_date=$(date +%F)
mv -f workdir/main.pdf Praca-Dyplomowa-Jakubczak-${current_date}.PDF

exit 1
fi

NAME=$1

cat <<EOT > workdir/temp.tex
\documentclass[12pt, withmarginpar]{mwbk}
\include{defs}
\begin{document}
\include{$NAME}
\end{document}
EOT

pdflatex -output-directory=workdir workdir/temp.tex
pdflatex -output-directory=workdir workdir/temp.tex
mv -f workdir/temp.pdf preview/$NAME.pdf