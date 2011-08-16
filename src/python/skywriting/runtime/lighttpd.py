
from cherrypy.process import plugins
import subprocess
import os.path
import tempfile
import shutil

class LighttpdAdapter(plugins.SimplePlugin):

    def __init__(self, bus, static_content_root, local_port):
        self.local_port = local_port
        self.static_content_root = static_content_root
        self.lighty_ancillary_dir = tempfile.mkdtemp(prefix=os.getenv('TEMP', default='/tmp/ciel-lighttpd-'))
        self.lighty_conf = os.path.join(self.lighty_ancillary_dir, "ciel-lighttpd.conf")
        self.socket_path = os.path.join(self.lighty_ancillary_dir, "ciel-socket")
        self.bus = bus

    def subscribe(self):
        self.bus.subscribe("start", self.start, 75)
        self.bus.subscribe("stop", self.stop, 10)

    def start(self):
        with open(self.lighty_conf, "w") as conf_out:
            print >>conf_out, """
# Skywriting lighttpd configuration file

## modules to load
# at least mod_access and mod_accesslog should be loaded
# all other module should only be loaded if really neccesary
server.modules              = (
                                "mod_access",
                               "mod_fastcgi",
                                "mod_accesslog" )

## A static document-root. For virtual hosting take a look at the
## mod_simple_vhost module.
server.document-root        = "{ciel_static_content}"

## where to send error-messages to
server.errorlog             = "{ciel_log}/lighttpd.log"

# files to check for if .../ is requested
index-file.names            = ( "index.php", "index.html",
                                "index.htm", "default.htm" )

# mimetype mapping
mimetype.assign             = (
  ".rpm"          =>      "application/x-rpm",
  ".pdf"          =>      "application/pdf",
  ".sig"          =>      "application/pgp-signature",
  ".spl"          =>      "application/futuresplash",
  ".class"        =>      "application/octet-stream",
  ".ps"           =>      "application/postscript",
  ".torrent"      =>      "application/x-bittorrent",
  ".dvi"          =>      "application/x-dvi",
  ".gz"           =>      "application/x-gzip",
  ".pac"          =>      "application/x-ns-proxy-autoconfig",
  ".swf"          =>      "application/x-shockwave-flash",
  ".tar.gz"       =>      "application/x-tgz",
  ".tgz"          =>      "application/x-tgz",
  ".tar"          =>      "application/x-tar",
  ".zip"          =>      "application/zip",
  ".mp3"          =>      "audio/mpeg",
  ".m3u"          =>      "audio/x-mpegurl",
  ".wma"          =>      "audio/x-ms-wma",
  ".wax"          =>      "audio/x-ms-wax",
  ".ogg"          =>      "application/ogg",
  ".wav"          =>      "audio/x-wav",
  ".gif"          =>      "image/gif",
  ".jar"          =>      "application/x-java-archive",
  ".jpg"          =>      "image/jpeg",
  ".jpeg"         =>      "image/jpeg",
  ".png"          =>      "image/png",
  ".xbm"          =>      "image/x-xbitmap",
  ".xpm"          =>      "image/x-xpixmap",
  ".xwd"          =>      "image/x-xwindowdump",
  ".css"          =>      "text/css",
  ".html"         =>      "text/html",
  ".htm"          =>      "text/html",
  ".js"           =>      "text/javascript",
  ".asc"          =>      "text/plain",
  ".c"            =>      "text/plain",
  ".cpp"          =>      "text/plain",
  ".log"          =>      "text/plain",
  ".conf"         =>      "text/plain",
  ".text"         =>      "text/plain",
  ".txt"          =>      "text/plain",
  ".dtd"          =>      "text/xml",
  ".xml"          =>      "text/xml",
  ".mpeg"         =>      "video/mpeg",
  ".mpg"          =>      "video/mpeg",
  ".mov"          =>      "video/quicktime",
  ".qt"           =>      "video/quicktime",
  ".avi"          =>      "video/x-msvideo",
  ".asf"          =>      "video/x-ms-asf",
  ".asx"          =>      "video/x-ms-asf",
  ".wmv"          =>      "video/x-ms-wmv",
  ".bz2"          =>      "application/x-bzip",
  ".tbz"          =>      "application/x-bzip-compressed-tar",
  ".tar.bz2"      =>      "application/x-bzip-compressed-tar",
  # default mime type
  ""              =>      "application/octet-stream",
 )

#### accesslog module
accesslog.filename          = "{ciel_log}/lighttpd-access.log"

## bind to port (default: 80)
server.port                = {ciel_port}

#### fastcgi module
## read fastcgi.txt for more info
## for PHP don't forget to set cgi.fix_pathinfo = 1 in the php.ini
fastcgi.server             = ( "/control/" =>
                               ( "cherrypy" =>
                                 (
                                   "socket" => "{ciel_socket}",
                   "max-procs" => 1,
                   "check-local" => "disable",
                   "docroot" => "/",
                                 )
                               )
                            )

# Disable stat-cache, as files are liable to change size under lighty
server.stat-cache-engine = "disable"
""".format(ciel_port=self.local_port, ciel_log=self.lighty_ancillary_dir, ciel_static_content=self.static_content_root, ciel_socket=self.socket_path)
        self.lighty_proc = subprocess.Popen(["lighttpd", "-D", "-f", self.lighty_conf])

    def stop(self):
        try:
            self.lighty_proc.kill()
            shutil.rmtree(self.lighty_ancillary_dir)
        except:
            pass
