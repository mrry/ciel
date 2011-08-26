Read me first
=============

CIEL is a distributed execution engine for task- and data-parallel
computation. It masks the complexity of distributed programming by
providing distributed storage, transparent inter-machine communciation
and transparent fault tolerance.

A CIEL job comprises one or more "tasks", each of which is associated
with an "executor". This package includes several generic executors
for integrating with UNIX utilities or programs that implement the
CIEL executor interface. In addition, the following executors can be
downloaded and installed separately:

* [Skywriting](https://github.com/mrry/ciel-skywriting) is a scripting
  language for use with CIEL. It provides a "distributed thread"
  abstraction that enables you to write simple scripts that spawn
  tasks using any executor, and use control-flow features such as
  loops and conditionals.

* [Java](https://github.com/mrry/ciel-java) support is provided by
  writing classes that implement a `Task` Java interface. There is
  also experimental support for using Scala to write distributed
  threads.

Installing CIEL
---------------

Before installing CIEL, you will need to install all of its
dependencies:

Before running CIEL, you may need to install various dependencies. We
develop CIEL on Ubuntu 10.04 and Fedora, and use the following packages;
however, these may vary based on the exact version of your operating
system.

+------------------------------------------------------+---------------------+---------------------+
| Packages                                             | `apt-get`           | `yum install`       |
|------------------------------------------------------|---------------------|---------------------|
| [Python 2.5+](http://www.python.org/)                | `python`            | `python`            |
| [httplib2](http://code.google.com/p/httplib2/)       | `python-httplib2`   | `python-httplib2`   |
| [simplejson](http://pypi.python.org/pypi/simplejson) | `python-simplejson` | `python-simplejson` |
| [CherryPY 3.1.2+](http://www.cherrypy.org/)          | `python-cherrypy3`  | `python-cherrypy3`  |
| [PycURL](http://pycurl.sourceforge.net/)             | `python-pycurl`     | `python-pycurl`     |
| [cURL](http://curl.haxx.se/)                         | `curl`              | `curl`              |
| [lighttpd](http://www.lighttpd.net/)                 | `lighttpd`          | `lighttpd`          |
| [flup](http://trac.saddi.com/flup)                   | `python-flup`       | `python-flup`       |
+------------------------------------------------------+---------------------+---------------------+

To install CIEL, use the `setup.py` script. For example, you may type
the following commands in the current directory:

```bash
    $ python setup.py build
    $ sudo python setup.py install
```

Running CIEL
------------

A CIEL cluster contains one "master" process and one or more "worker"
processes.

To start a master, use the following command:

```bash
$ ciel master
```

To start a worker on the same machine, use the following command:

```bash
$ ciel worker
```

To start a worker on a different machine, use the following command:

```bash
$ ciel worker -m http://${MASTER_HOSTNAME}:8000/
```

To find out about more options when running a master and workers, use
the following command:

```bash
$ ciel [master|worker] --help
```

Testing CIEL
------------

To run a simple CIEL job, first create a file called `test.pack` with
the following contents:

```
{"start":
    {"handler": "stdinout",
     "args":
        {"args": 
            {"command_line": ["echo", "Hello, world!"]}
        }
    },
 "result": [{"__decode_ref__": "noop"}]
}
```

This is a simple example of a "package file", which is used to specify
a CIEL job. Package files are the most flexible way of specifying a
CIEL job, but some executors (such as Skywriting) provide simpler ways
to create simple jobs.

To execute the package file, first start a cluster, then type the
following command:

```bash
$ ciel run test.pack
```