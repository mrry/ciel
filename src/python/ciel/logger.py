# Copyright (c) 2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
import logging
import sys
import datetime
import rfc822
import time

class CielLogger:
    
    def __init__(self, logger_root='ciel'):
        self.logger_root = logger_root
        self.log = logging.getLogger(self.logger_root)
        self.log.setLevel(logging.ERROR)
    
        log_handler = logging.StreamHandler(sys.stderr)
        self.log.addHandler(log_handler)
    
    def __call__(self, *args, **kwargs):
        """Write to the error log.
        
        This is not just for errors! Applications may call this at any time
        to log application-specific information.
        """
        return self.error(*args, **kwargs)
    
    def error(self, msg='', context='', severity=logging.INFO, traceback=False):
        """Write to the error log.
        
        This is not just for errors! Applications may call this at any time
        to log application-specific information.
        """
        if traceback:
            msg += format_exc()
        self.log.log(severity, ' '.join((self.time(), context, msg)))
    
    def time(self):
        return '[%f]' % (lambda t: (time.mktime(t.timetuple()) + t.microsecond / 1e6))(datetime.datetime.now())
        #"""Return now() in Apache Common Log Format (no timezone)."""
        #now = datetime.datetime.now()
        #month = rfc822._monthnames[now.month - 1].capitalize()
        #return ('[%02d/%s/%04d:%02d:%02d:%02d]' %
        #        (now.day, month, now.year, now.hour, now.minute, now.second))
    
def format_exc(exc=None):
    """Return exc (or sys.exc_info if None), formatted."""
    if exc is None:
        exc = sys.exc_info()
    if exc == (None, None, None):
        return ""
    import traceback
    return "".join(traceback.format_exception(*exc))