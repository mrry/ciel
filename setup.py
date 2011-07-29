from setuptools import setup

setup(
    name = "ciel",
    version = '0.1-dev',
    description = "Execution engine for distributed, parallel computation",
    author = "Derek Murray",
    author_email = "Derek.Murray@cl.cam.ac.uk",
    url = "http://www.cl.cam.ac.uk/netos/ciel/",
    packages = [ 'skywriting', 'skywriting.runtime',
                 'skywriting.runtime.master', 'skywriting.runtime.worker',
                 'skywriting.runtime.executors', 'skywriting.runtime.util', 'ciel', 'shared' ],
    package_dir = { '' : 'src/python' },
    scripts = [ "scripts/%s" %s for s in
                  ['ciel-killall.sh', 'ciel-kill-cluster', 'ciel-launch-cluster',
                   'ciel-launch-master', 'ciel-launch-worker', 'ciel-poll-job',
                   'ciel-print-job-result', 'ciel-run-job', 'ciel-run-job-async',
                   'ciel-task-crawler', 'sw-job', 'sw-master', 'sw-start-job', 
                   'sw-worker'] ],
    data_files = [ ("share/ciel/", ["src/python/skywriting/runtime/lighttpd.conf"]) ],
    classifiers = [
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'Intended Audience :: Science/Research',
            'License :: OSI Approved :: ISC License (ISCL)',
            'Operating System :: POSIX',
            'Topic :: Software Development :: Interpreters',
            'Topic :: System :: Distributed Computing',
        ],
    requires=['simplejson', 'CherryPy (>=3.1.0)', 'ply', 'pycurl', 'httplib2' ]
)

