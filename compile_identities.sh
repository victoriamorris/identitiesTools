C:/python34/python setup.py install
C:/python34/python setup.py py2exe
mv dist/identities_graph.exe identities_graph.exe
rmdir dist
rm bin/__pycache__/identities_graph.cpython-34.pyc
rmdir bin/__pycache__
rm -rf build
echo Hello