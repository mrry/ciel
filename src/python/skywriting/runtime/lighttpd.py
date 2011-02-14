
from cherrypy.process import plugins
import subprocess
import os.path
import tempfile

class LighttpdAdapter(plugins.SimplePlugin):

    def __init__(self, bus, lighty_conf_template, static_content_root, local_port):
        self.local_port = local_port
        self.static_content_root = static_content_root
        self.lighty_conf_template = lighty_conf_template
        self.lighty_ancillary_dir = tempfile.mkdtemp(prefix=os.getenv('TEMP', default='/tmp/ciel-lighttpd-'))
        self.lighty_conf = os.path.join(self.lighty_ancillary_dir, "ciel-lighttpd.conf")
        self.socket_path = os.path.join(self.lighty_ancillary_dir, "ciel-socket")
        self.bus = bus

    def subscribe(self):
        self.bus.subscribe("start", self.start, 75)
        self.bus.subscribe("stop", self.stop, 10)

    def start(self):
        with open(self.lighty_conf_template, "r") as conf_in:
            with open(self.lighty_conf, "w") as conf_out:
                m4_args = ["m4", "-DCIEL_PORT=%d" % self.local_port, 
                           "-DCIEL_LOG=%s" % self.lighty_ancillary_dir, 
                           "-DCIEL_STATIC_CONTENT=%s" % self.static_content_root,
                           "-DCIEL_SOCKET=%s" % self.socket_path]
                subprocess.check_call(m4_args, stdin=conf_in, stdout=conf_out)
        self.lighty_proc = subprocess.Popen(["lighttpd", "-D", "-f", self.lighty_conf])

    def stop(self):
        try:
            self.lighty_proc.kill()
        except:
            pass
