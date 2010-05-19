from distutils.core import setup

setup(
    name = "skywriting",
    version = '0.1-dev',
    description = "Programming language for distributed, parallel computation",
    author = "Derek Murray",
    author_email = "derek.murray@cl.cam.ac.uk",
    url = "http://www.cl.cam.ac.uk/research/srg/netos/skywriting/",
    packages = [ 'mrry', 'mrry.mercator', 'mrry.mercator.cloudscript', 'mrry.mercator.runtime',
                 'mrry.mercator.runtime.master', 'mrry.mercator.runtime.worker',
                 'mrry.mercator.runtime.interactive' ],
    package_dir = { '' : 'src/python' },
    scripts = [ 'scripts/sw-master', 'scripts/sw-worker', 'scripts/sw-job', 'scripts/sw-console' ],
    classifiers = [
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'Intended Audience :: Science/Research',
            'License :: OSI Approved :: ISC License (ISCL)',
            'Operating System :: POSIX',
            'Topic :: Software Development :: Interpreters',
            'Topic :: System :: Distributed Computing',
        ],
    requires=['simplejson', 'CherryPy (>=3.1.0)' ]
)

