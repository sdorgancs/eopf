source env.sh
cd docs
sphinx-apidoc -o source/ ../ -f -d 10
make html
cd -