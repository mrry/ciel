
from skywriting.runtime.pycurl_thread import pycURLContext
import pycurl
import urlparse
import ciel
import logging

class pycURLFetchContext(pycURLContext):

    def __init__(self, dest_fp, src_url, multi, result_callback, progress_callback=None, start_byte=None):

        pycURLContext.__init__(self, src_url, multi, result_callback)

        self.description = src_url
        self.progress_callback = None

        self.curl_ctx.setopt(pycurl.WRITEDATA, dest_fp)
        if progress_callback is not None:
            self.curl_ctx.setopt(pycurl.NOPROGRESS, False)
            self.curl_ctx.setopt(pycurl.PROGRESSFUNCTION, self.progress)
            self.progress_callback = progress_callback
        if start_byte is not None and start_byte != 0:
            self.curl_ctx.setopt(pycurl.HTTPHEADER, ["Range: bytes=%d-" % start_byte])

    def success(self):
        self.progress_callback(self.curl_ctx.getinfo(pycurl.SIZE_DOWNLOAD))
        pycURLContext.success(self)

    def progress(self, toDownload, downloaded, toUpload, uploaded):
        self.progress_callback(downloaded)

class FileTransferContext:

    def __init__(self, urls, save_filename, multi, callbacks):
        self.urls = urls
        self.multi = multi
        self.save_filename = save_filename
        self.callbacks = callbacks
        self.failures = 0
        self.cancelled = False
        self.curl_fetch = None

    def start_next_attempt(self):
        self.fp = open(self.save_filename, "w")
        ciel.log("Starting fetch attempt %d using %s" % (self.failures + 1, self.urls[self.failures]), "CURL_FETCH", logging.INFO)
        self.curl_fetch = pycURLFetchContext(self.fp, self.urls[self.failures], self.multi, self.result, self.callbacks.progress)
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
                ciel.log.error('Fetch %s: no more URLs to try.' % self.save_filename, 'BLOCKSTORE', logging.INFO)
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

    def get_filename(self):
        return self.save_filename

    def wrote_file(self):
        return True

    def cancel(self):
        ciel.log("Fetch %s: cancelling" % self.save_filename, "CURL_FETCH", logging.INFO)
        self.cancelled = True
        if self.curl_fetch is not None:
            self.curl_fetch.cancel()
        self.fp.close()
        self.callbacks.result(False)

class StreamTransferContext:

    def __init__(self, ref, block_store, callbacks):
        self.url = block_store.get_fetch_urls_for_ref(ref)[0]
        parsed_url = urlparse.urlparse(self.url)
        self.worker_netloc = parsed_url.netloc
        self.ref = ref
        self.save_filename = block_store.fetch_filename(ref.id)
        open(self.save_filename, "w").close()
        self.callbacks = callbacks
        self.current_data_fetch = None
        self.previous_fetches_bytes_downloaded = 0
        self.remote_done = False
        self.remote_failed = False
        self.latest_advertisment = 0
        self.block_store = block_store
        self.cancelled = False
        self.current_chunk_size = None

    def start_next_fetch(self):
        ciel.log("Stream-fetch %s: start fetch from byte %d" % (self.ref.id, self.previous_fetches_bytes_downloaded), "CURL_FETCH", logging.INFO)
        self.current_data_fetch = pycURLFetchContext(self.fp, self.url, self.block_store.fetch_thread, self.result, self.progress, self.previous_fetches_bytes_downloaded)
        self.current_data_fetch.start()

    def start(self):
        self.fp = open(self.save_filename, "w")
        self.start_next_fetch()
        self.block_store.add_incoming_stream(self.ref.id, self)

    def progress(self, bytes_downloaded):
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
            ciel.log("Stream-fetch %s: complete" % self.ref.id, "CURL_FETCH", logging.INFO)
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
        self.fp.close()
        self.block_store.remove_incoming_stream(self.ref.id)
        self.callbacks.result(success)

    def _socket_attempt_completed(self, success, socket):
        if not success:
            ciel.log("Stream-fetch %s: TCP transfer failed; falling back to HTTP" % self.ref.id, "CURL_FETCH", logging.INFO)
            self.initial_socket_attempt = False
            self.start()
            self.subscribe_remote_output(self.current_chunk_size)
        else:
            ciel.log("Stream-fetch %s: TCP transfer started" % self.ref.id, "CURL_FETCH", logging.INFO)

            self.callbacks.result(True)
            self.set_filename(fifo_name)

    def socket_attempt_completed(self, success, socket):
        self.block_store.fetch_thread.do_from_curl_thread(lambda: self._socket_attempt_completed(success, socket))

    def subscribe_result(self, success, _):
        if not success:
            ciel.log("Stream-fetch %s: failed to subscribe to remote adverts. Abandoning stream." % self.ref.id, "CURL_FETCH", logging.INFO)
            self.remote_failed = True
            if self.current_data_fetch is None:
                self.complete(False)

    def subscribe_remote_output(self, chunk_size):
        ciel.log("Stream-fetch %s: change notification chunk size to %d" % (self.ref.id, chunk_size), "CURL_FETCH", logging.INFO)
        post_data = simplejson.dumps({"netloc": self.block_store.netloc, "chunk_size": chunk_size})
        self.block_store._post_string_noreturn("http://%s/control/streamstat/%s/subscribe" % (self.worker_netloc, self.ref.id), post_data, result_callback=self.subscribe_result)

    def set_chunk_size(self, new_chunk_size):
        if new_chunk_size != self.current_chunk_size:
            self.subscribe_remote_output(new_chunk_size)
        self.current_chunk_size = new_chunk_size

    def cancel(self):
        ciel.log("Stream-fetch %s: cancelling" % self.ref.id, "CURL_FETCH", logging.INFO)
        self.cancelled = True
        post_data = simplejson.dumps({"netloc": self.block_store.netloc})
        self.block_store._post_string_noreturn("http://%s/control/streamstat/%s/unsubscribe" % (self.worker_netloc, self.ref.id), post_data)
        if self.current_data_fetch is not None:
            self.current_data_fetch.cancel()
        self.callbacks.result(False)

    def advertisment(self, bytes=None, done=None, absent=None, failed=None):
        if self.cancelled:
            return
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
            if self.latest_advertisment < bytes:
                self.latest_advertisment = bytes
            else:
                ciel.log("Stream-fetch %s: intriguing anomaly: advert for %d bytes; currently have %d. Probable reordering in the network" % (self.ref.id, bytes, self.latest_advertisment), "CURL_FETCH", logging.WARNING)
            if self.remote_done and not done:
                ciel.log("Stream-fetch %s: intriguing anomaly: advert said not-done, but we are. Probable reordering in the network" % self.ref.id, "CURL_FETCH", logging.WARNING)
            self.remote_done = self.remote_done or done
            if self.current_data_fetch is None:
                self.check_complete()


class GlobalHttpFetchInProgress:
        
    def __init__(self, ref, block_store):
        self.listeners = []
        self.last_progress = 0
        self.ref = ref
        self.block_store = block_store
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
        self.block_store.fetch_completed(self.ref, success and self.fetch_context.wrote_file())
        for l in self.listeners:
            l.result(success)

    def reset(self):
        for l in self.listeners:
            l.reset()

    def update_chunk_size(self):
        interested_listeners = filter(lambda x: x.chunk_size is not None, self.listeners)
        if len(interested_listeners) != 0:
            interested_listeners.sort(key=lambda x: x.chunk_size)
            self.fetch_context.set_chunk_size(interested_listeners[0].chunk_size)

    def _unsubscribe(self, fetch_client):
        if self.completed:
            ciel.log("Removing fetch client %s: transfer had already completed" % fetch_client, "CURL_FETCH", logging.WARNING)
            return
        self.listeners.remove(fetch_client)
        self.update_chunk_size()
        fetch_client.result(False)
        if len(self.listeners) == 0:
            ciel.log("Removing fetch client %s: no clients remain, cancelling transfer" % fetch_client, "CURL_FETCH", logging.INFO)
            self.fetch_context.cancel()

    def unsubscribe(self, fetch_client):
        # Asynchronous out-of-thread callback: might come from the cURL thread or any other.
        do_from_curl_thread(lambda: self._unsubscribe(fetch_client))

    def add_listener(self, fetch_client):
        self.listeners.append(fetch_client)
        fetch_client.progress(self.last_progress)
        self.update_chunk_size()

    def get_filename(self):
        return self.fetch_context.get_filename()

    def get_completed_ref(self, is_sweetheart):
        if not self.fetch_context.wrote_file():
            return SW2_CompletedReference(self.ref.id)
        else:
            if is_sweetheart:
                return SW2_SweetheartReference(self.ref.id, self.last_progress, self.block_store.netloc, [self.block_store.netloc])
            else:
                return SW2_ConcreteReference(self.ref.id, self.last_progress, [self.block_store.netloc])

    def get_stream_ref(self):
        raise Exception("Stream-refs from fetches don't work right now")
        #return SW2_StreamReference(self.ref.id, [self.block_store.netloc])
