from distutils.core import setup

setup(
    name = "skywriting",
    version = '0.1-dev',
    description = "Programming language for distributed, parallel computation",
    author = "Derek Murray",
    author_email = "derek.murray@cl.cam.ac.uk",
    url = "http://www.cl.cam.ac.uk/research/srg/netos/skywriting/",
    packages = [ 'mrry.mercator', 'mrry.mercator.cloudscript', 'mrry.mercator.runtime' ],
    package_dir = { '' : 'src/python' }
)

