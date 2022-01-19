import sys
try:
    import py2exe
except:
    raw_input('Please install py2exe first...')
    sys.exit(-1)

from distutils.core import setup
import shutil

sys.argv.append('py2exe')

setup(
    options={
        'py2exe': {'bundle_files': 1, 'compressed': True}
    },
    console=[
        {'script': "main.py",
        "icon_resources": [(0, "app.ico")]},   
    ],
    zipfile=None,
)

shutil.move('dist\\EeLogEqtl.exe', '.\\EeLogEqtl.exe')
shutil.rmtree('dist')
shutil.rmtree('build')
