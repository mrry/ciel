
from skywriting.runtime.pycurl_thread import pycURLContext, do_from_curl_thread
from skywriting.runtime.block_store import get_fetch_urls_for_ref, create_fetch_file_for_ref, get_own_netloc
from shared.references import SW2_ConcreteReference, SW2_FetchReference
import skywriting.runtime.remote_stat as remote_stat

import pycurl
import urlparse
import ciel
import logging

# pycURLFetches currently in operation
active_http_transfers = dict()

class pycURLFetchContext(pycURLContext):

    def __init__(self, dest_fp, src_url, result_callback, progress_callback=None, start_byte=None, fetch_client=None):

        pycURLContext.__init__(self, src_url, result_callback)

        self.description = src_url
        self.progress_callback = None

        self.fetch_client = fetch_client

        self.curl_ctx.setopt(pycurl.WRITEDATA, dest_fp)
        if progress_callback is not None:
            self.curl_ctx.setopt(pycurl.NOPROGRESS, False)
            self.curl_ctx.setopt(pycurl.PROGRESSFUNCTION, self.progress)
            self.progress_callback = progress_callback
        if start_byte is not None and start_byte != 0:
            self.curl_ctx.setopt(pycurl.HTTPHEADER, ["Range: bytes=%d-" % start_byte])

    def success(self):
        bytes_downloaded = self.curl_ctx.getinfo(pycurl.SIZE_DOWNLOAD)
        self.progress_callback(bytes_downloaded)
        pycURLContext.success(self)
        if self.fetch_client.task_record is not None:
            self.fetch_client.task_record.add_completed_fetch(self.description, bytes_downloaded)

    def progress(self, toDownload, downloaded, toUpload, uploaded):
        self.progress_callback(downloaded)

class FileTransferContext:

    def __init__(self, ref, callbacks, fetch_client):
        self.ref = ref
        self.urls = get_fetch_urls_for_ref(self.ref)
        self.callbacks = callbacks
        self.failures = 0
        self.cancelled = False
        self.curl_fetch = None
        self.fetch_client = fetch_client

    def start_next_attempt(self):
        self.fp = open(self.callbacks.bs_ctx.filename, "w")
        ciel.log("Starting fetch attempt %d using %s" % (self.failures + 1, self.urls[self.failures]), "CURL_FETCH", logging.INFO)
        self.curl_fetch = pycURLFetchContext(self.fp, self.urls[self.failures], self.result, self.callbacks.progress, fetch_client=self.fetch_client)
        self.curl_fetch.start()

    def start(self):
        self.start_next_attempt()

    def result(self, success):
        self.fp.close()
        if success:
            self.callbacks.result(True)
        else:
            self.failures += 1
            if self.failures == len(self.urls):
                ciel.log.error('Fetch %s: no more URLs to try.' % self.ref.id, 'BLOCKSTORE', logging.INFO)
                self.callbacks.result(False)
            else:
                ciel.log.error("Fetch %s failed; trying next URL" % (self.urls[self.failures - 1]))
                self.curl_fetch = None
                self.callbacks.reset()
                if not self.cancelled:
                    self.start_next_attempt()

    def set_chunk_size(self, new_chunk_size):
        # Don't care: we always request the whole file.
        pass

    def cancel(self):
        ciel.log("Fetch %s: cancelling" % self.ref.id, "CURL_FETCH", logging.INFO)
        self.cancelled = True
        if self.curl_fetch is not None:
            self.curl_fetch.cancel()
        self.fp.close()
        self.callbacks.result(False)

class StreamTransferContext:

    def __init__(self, ref, callbacks, fetch_client):
        self.url = get_fetch_urls_for_ref(ref)[0]
        parsed_url = urlparse.urlparse(self.url)
        self.worker_netloc = parsed_url.netloc
        self.ref = ref
        self.fetch_client = fetch_client
        open(callbacks.bs_ctx.filename, "w").close()
        self.callbacks = callbacks
        self.current_data_fetch = None
        self.previous_fetches_bytes_downloaded = 0
        self.remote_done = False
        self.remote_failed = False
        self.latest_advertisment = 0
        self.cancelled = False
        self.local_done = False
        self.current_chunk_size = None
        self.subscribed_to_remote_adverts = True
        
    def start_next_fetch(self):
        ciel.log("Stream-fetch %s: start fetch from byte %d" % (self.ref.id, self.previous_fetches_bytes_downloaded), "CURL_FETCH", logging.INFO)
        self.current_data_fetch = pycURLFetchContext(self.fp, self.url, self.result, self.progress, self.previous_fetches_bytes_downloaded, fetch_client=self.fetch_client)
        self.current_data_fetch.start()

    def start(self):
        self.fp = open(self.callbacks.bs_ctx.filename, "w")
        self.start_next_fetch()

    def progress(self, bytes_downloaded):
        self.fp.flush()
        self.callbacks.progress(self.previous_fetches_bytes_downloaded + bytes_downloaded)

    def consider_next_fetch(self):
        if self.remote_done or self.latest_advertisment - self.previous_fetches_bytes_downloaded > self.current_chunk_size:
            self.start_next_fetch()
        else:
            ciel.log("Stream-fetch %s: paused (remote has %d, I have %d)" % 
                     (self.ref.id, self.latest_advertisment, self.previous_fetches_bytes_downloaded), 
                     "CURL_FETCH", logging.INFO)
            self.current_data_fetch = None

    def check_complete(self):
        if self.remote_done and self.latest_advertisment == self.previous_fetches_bytes_downloaded:
            self.complete(True)
        else:
            self.consider_next_fetch()

    def result(self, success):
        # Current transfer finished.
        if self.remote_failed:
            ciel.log("Stream-fetch %s: transfer completed, but failure advertised in the meantime" % self.ref.id, "CURL_FETCH", logging.WARNING)
            self.complete(False)
            return
        if not success:
            ciel.log("Stream-fetch %s: transfer failed" % self.ref.id)
            self.complete(False)
        else:
            this_fetch_bytes = self.current_data_fetch.curl_ctx.getinfo(pycurl.SIZE_DOWNLOAD)
            ciel.log("Stream-fetch %s: transfer succeeded (got %d bytes)" % (self.ref.id, this_fetch_bytes),
                     "CURL_FETCH", logging.INFO)
            self.previous_fetches_bytes_downloaded += this_fetch_bytes
            self.check_complete()

    def complete(self, success):
        if not self.local_done:
            self.local_done = True
            ciel.log("Stream-fetch %s: complete" % self.ref.id, "CURL_FETCH", logging.INFO)
            self.unsubscribe_remote_output()
            self.fp.close() 
            self.callbacks.result(success)        

    # Sneaky knowledge here: this call comes from the cURL thread.
    def subscribe_result(self, success, _):
        if not success:
            ciel.log("Stream-fetch %s: failed to subscribe to remote adverts. Abandoning stream." 
                     % self.ref.id, "CURL_FETCH", logging.INFO)
            self.subscribed_to_remote_adverts = False
            self.remote_failed = True
            if self.current_data_fetch is None:
                self.complete(False)

    def unsubscribe_remote_output(self):
        if self.subscribed_to_remote_adverts:
            remote_stat.unsubscribe_remote_output(self.ref.id)
            self.subscribed_to_remote_adverts = False

    def subscribe_remote_output(self, chunk_size):
        ciel.log("Stream-fetch %s: change notification chunk size to %d" 
                 % (self.ref.id, chunk_size), "CURL_FETCH", logging.INFO)
        remote_stat.subscribe_remote_output(self.ref.id, self.worker_netloc, chunk_size, self)

    def set_chunk_size(self, new_chunk_size):
        if new_chunk_size != self.current_chunk_size:
            self.subscribe_remote_output(new_chunk_size)
        self.current_chunk_size = new_chunk_size

    def cancel(self):
        ciel.log("Stream-fetch %s: cancelling" % self.ref.id, "CURL_FETCH", logging.INFO)
        self.cancelled = True
        if self.current_data_fetch is not None:
            self.current_data_fetch.cancel()
        self.complete(False)

    def _advertisment(self, bytes=None, done=None, absent=None, failed=None):
        if self.cancelled:
            return
        if done or absent or failed:
            self.subscribed_to_remote_adverts = False
        if absent is True or failed is True:
            if absent is True:
                ciel.log("Stream-fetch %s: advertisment subscription reported file absent" % self.ref.id, "CURL_FETCH", logging.WARNING)
            else:
                ciel.log("Stream-fetch %s: advertisment reported remote production failure" % self.ref.id, "CURL_FETCH", logging.WARNING)
            self.remote_failed = True
            if self.current_data_fetch is None:
                self.complete(False)
        else:
            ciel.log("Stream-fetch %s: got advertisment: bytes %d done %s" % (self.ref.id, bytes, done), "CURL_FETCH", logging.INFO)
            if self.latest_advertisment <= bytes:
                self.latest_advertisment = bytes
            else:
                ciel.log("Stream-fetch %s: intriguing anomaly: advert for %d bytes; currently have %d. Probable reordering in the network" % (self.ref.id, bytes, self.latest_advertisment), "CURL_FETCH", logging.WARNING)
            if self.remote_done and not done:
                ciel.log("Stream-fetch %s: intriguing anomaly: advert said not-done, but we are. Probable reordering in the network" % self.ref.id, "CURL_FETCH", logging.WARNING)
            self.remote_done = self.remote_done or done
            if self.current_data_fetch is None:
                self.check_complete()

    def advertisment(self, *pargs, **kwargs):
        do_from_curl_thread(lambda: self._advertisment(*pargs, **kwargs))

# Represents pycURL doing a fetch, with potentially many clients listening to the results.
class pycURLFetchInProgress:
        
    def __init__(self, ref):
        self.listeners = []
        self.last_progress = 0
        self.ref = ref
        self.bs_ctx = create_fetch_file_for_ref(self.ref)
        self.chunk_size = None
        self.completed = False

    def set_fetch_context(self, fetch_context):
        self.fetch_context = fetch_context

    def progress(self, bytes):
        for l in self.listeners:
            l.progress(bytes)
        self.last_progress = bytes

    def result(self, success):
        self.completed = True
        del active_http_transfers[self.ref.id]
        if success:
            ref = SW2_ConcreteReference(self.ref.id, self.last_progress, [get_own_netloc()])
            self.bs_ctx.commit()
        else:
            ref = None
        for l in self.listeners:
            l.result(success, ref)

    def reset(self):
        for l in self.listeners:
            l.reset()

    def update_chunk_size(self):
        interested_listeners = filter(lambda x: x.chunk_size is not None, self.listeners)
        if len(interested_listeners) != 0:
            interested_listeners.sort(key=lambda x: x.chunk_size)
            self.fetch_context.set_chunk_size(interested_listeners[0].chunk_size)

    def unsubscribe(self, fetch_client):
        if self.completed:
            ciel.log("Removing fetch client %s: transfer had already completed" % fetch_client, "CURL_FETCH", logging.WARNING)
            return
        self.listeners.remove(fetch_client)
        self.update_chunk_size()
        fetch_client.result(False)
        if len(self.listeners) == 0:
            ciel.log("Removing fetch client %s: no clients remain, cancelling transfer" % fetch_client, "CURL_FETCH", logging.INFO)
            self.fetch_context.cancel()

    def add_listener(self, fetch_client):
        self.listeners.append(fetch_client)
        fetch_client.progress(self.last_progress)
        self.update_chunk_size()

# Represents a single client to an HTTP fetch. 
# Also proxies calls so that everything from here on up is in the cURL thread.
class HttpTransferContext:
    
    def __init__(self, ref, fetch_client):
        self.ref = ref
        self.fetch_client = fetch_client
        self.fetch = None

    def start_http_fetch(self):
        new_fetch = pycURLFetchInProgress(self.ref)
        if isinstance(self.ref, SW2_ConcreteReference) or isinstance(self.ref, SW2_FetchReference):
            new_ctx = FileTransferContext(self.ref, new_fetch, self.fetch_client)
        else:
            new_ctx = StreamTransferContext(self.ref, new_fetch, self.fetch_client)
        new_fetch.set_fetch_context(new_ctx)
        active_http_transfers[self.ref.id] = new_fetch
        new_ctx.start()
        
    def _start(self):
        if self.ref.id in active_http_transfers:
            ciel.log("Joining existing fetch for ref %s" % self.ref, "BLOCKSTORE", logging.INFO)
        else:
            self.start_http_fetch()
        active_http_transfers[self.ref.id].add_listener(self.fetch_client)
        self.fetch = active_http_transfers[self.ref.id]
        self.fetch_client.set_filename(self.fetch.bs_ctx.filename, False)

    def start(self):
        do_from_curl_thread(lambda: self._start())

    def _unsubscribe(self):
        self.fetch.unsubscribe(self.fetch_client)

    def unsubscribe(self, fetcher):
        do_from_curl_thread(lambda: self._unsubscribe())

