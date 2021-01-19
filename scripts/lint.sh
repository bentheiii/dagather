# run various linters
set -e
flake8 --max-line-length 120 dagather
set +e
python -c "import sys; sys.exit(sys.version_info < (3,9,0))"
res=$?
set -e
if [ "$res" -eq "0" ]
  then
    echo "pytype not run, please run in python 3.8 or lower"
  else
    pytype --keep-going dagather
fi
