:: Upgrade pip & setuptools
python -m pip install --upgrade pip setuptools

::Install prebuild wheel
@REM python -m pip install https://pip.vnpy.com/colletion/TA_Lib-0.4.17-cp37-cp37m-win_amd64.whl
@REM python -m pip install https://pip.vnpy.com/colletion/quickfix-1.15.1-cp37-cp37m-win_amd64.whl

::Install Python Modules
python -m pip install -r requirements-test.txt

:: Install vn.py
python -m pip install .