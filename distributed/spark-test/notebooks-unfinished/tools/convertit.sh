src=$1
dest=${1%%.scala}.ipynb

echo $dest
LC_CTYPE=C && LANG=C && sed -n -f cvt.sed "$src" > "$dest"
