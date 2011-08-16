from setuptools import setup

setup(
    name = "ciel",
    version = '0.1',
    description = "Execution engine for distributed, parallel computation",
    author = "Derek Murray",
    author_email = "Derek.Murray@cl.cam.ac.uk",
    url = "http://www.cl.cam.ac.uk/netos/ciel/",
    packages = [ 'ciel', 'ciel.runtime',
                 'ciel.runtime.master', 'ciel.runtime.worker',
                 'ciel.runtime.executors', 'ciel.runtime.util', 'shared', 'fcpy' ],
    package_dir = { '' : 'src/python' },
    entry_points= { 'console_scripts': ['ciel = ciel.cli:main' ]},
#    scripts = [ "scripts/%s" %s for s in
#                  ['ciel-launch-master', 'ciel-launch-worker', 'ciel-poll-job',
#                   'ciel-print-job-result', 'ciel-run-job', 'ciel-run-job-async',
#                   'ciel-task-crawler', 'sw-job', 'sw-start-job' ] ],
    classifiers = [
            'Development Status :: 3 - Alpha',
            'Environment :: Console',
            'Environment :: Web Environment',
            'Intended Audience :: Developers',
            'Intended Audience :: Information Technology',
            'Intended Audience :: Science/Research',
            'Intended Audience :: System Administrators',
            'License :: OSI Approved :: ISC License (ISCL)',
            'Operating System :: POSIX',
            'Topic :: System :: Clustering',
            'Topic :: System :: Distributed Computing',
        ],
    requires=['simplejson', 'CherryPy (>=3.1.0)', 'pycurl', 'httplib2' ]
)

