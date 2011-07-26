from setuptools import setup

setup(
    name = "skywriting",
    version = '0.1-dev',
    description = "Programming language for distributed, parallel computation",
    author = "Derek Murray",
    author_email = "derek.murray@cl.cam.ac.uk",
    url = "http://www.cl.cam.ac.uk/research/srg/netos/skywriting/",
    packages = [ 'skywriting', 'skywriting.lang', 'skywriting.runtime',
                 'skywriting.runtime.master', 'skywriting.runtime.worker',
                 'skywriting.runtime.util', 'ciel', 'shared' ],
    package_dir = { '' : 'src/python' },
    scripts = [ 'scripts/sw-master', 'scripts/sw-worker', 'scripts/sw-job',
                'scripts/sw-console', 'scripts/run_master.sh',
                'scripts/run_worker.sh', 'scripts/sw-start-job',
                'scripts/run_job.sh' ],
    data_files = [ ("share/ciel/skyweb", ['src/js/skyweb/%s' % s
                                          for s in
                                          ['jquery-1.4.2.js',
                                           'jquery.query-2.1.7.js',
                                           'raphael-min.js',
                                           'skyweb.css',
                                           'skyweb.js',
                                           'skyweb-test.html']] ),
                   ("share/ciel", ["src/python/skywriting/runtime/lighttpd.conf"]),
                   ("share/ciel/skywriting",
                    ["src/sw/stdlib/%s" %s for s in
                     ["environ", "grab", "java", "mapreduce",
                      "stdinout", "sync"]])],
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

