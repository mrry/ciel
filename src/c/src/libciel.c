
#define _GNU_SOURCE

#include <jansson.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <errno.h>

static FILE* ciel_out;
static FILE* ciel_in;

static char* ciel_socket_name;
static int ciel_socket_fd;

void ciel_json_error(char* string, json_error_t* error) {

  if(!string)
    string = "(no string)";
  fprintf(stderr, "Error parsing %s: error=%s, source=%s, line=%d, column=%d", string, error->text, error->source, error->line, error->column);

}

void ciel_read_all(FILE* fin, int length, char* dest) {

  int bytes_read = 0;
  while(bytes_read < length) {
    size_t this_read = fread(dest + bytes_read, 1, length - bytes_read, fin);
    if(this_read == 0) {
      if(feof(fin)) {
	fprintf(stderr, "Unexpected EOF reading!\n");
	exit(1);
      }
      else if(ferror(fin)) {
	fprintf(stderr, "Error reading!\n");
	exit(1);
      }
    }
    bytes_read += this_read;
  }

}

void ciel_write_all(FILE* fout, char* buf, int len) {

  size_t bytes_written = 0;
  while(bytes_written < len) {
    size_t this_write = fwrite(buf + bytes_written, 1, len - bytes_written, fout);
    if(this_write == 0) {
      fprintf(stderr, "Error writing\n");
      exit(1);
    }
    bytes_written += this_write;
  }

}

json_t* ciel_read_framed_json(FILE* fin) {

  json_error_t error_bucket;
  unsigned int buffer_size;

  printf("Read JSON...\n");

  ciel_read_all(fin, 4, (char*)&buffer_size);

  buffer_size = ntohl(buffer_size);
  printf("Reading %u bytes\n", buffer_size);
  
  char* json_buffer = malloc(buffer_size + 1);
  if(!json_buffer) {
    fprintf(stderr, "OOM reading JSON\n");
    exit(1);
  }
  
  ciel_read_all(fin, buffer_size, json_buffer);
  json_buffer[buffer_size] = '\0';
  printf("Got JSON: %s\n", json_buffer);

  json_t* json_root = json_loads(json_buffer, 0, &error_bucket);
  if(!json_root)
    ciel_json_error(json_buffer, &error_bucket);

  printf("JSON load complete\n");

  free(json_buffer);
  return json_root;

}

void ciel_write_framed_json(json_t* json, FILE* fout) {

  char* json_str = json_dumps(json, 0);
  if(!json_str) {
    fprintf(stderr, "ciel_write_framed_json: dumps failure\n");
    exit(1);
  }

  printf("Writing JSON: %s\n", json_str);

  unsigned int len = strlen(json_str);
  unsigned int nbo_len = htonl(len);
  
  ciel_write_all(fout, (char*)&nbo_len, 4);
  ciel_write_all(fout, json_str, len);

  fflush(fout);

  printf("Write complete\n");
  
  free(json_str);

}

int ciel_receive_fd() {

  struct sockaddr_un otherend_addr;
  int sock_len = sizeof(otherend_addr);
  int rcv_sock = accept(ciel_socket_fd, &otherend_addr, &sock_len);
  if(rcv_sock == -1) {
    fprintf(stderr, "Failed to accept receiving an FD\n");
    return -1;
  }
  
  char buf[3];
  
  struct iovec recv_vec;
  recv_vec.iov_base = buf;
  recv_vec.iov_len = 2;
  
  char control_buf[CMSG_SPACE(sizeof(int))];
  
  struct msghdr to_recv;
  to_recv.msg_name = 0;
  to_recv.msg_namelen = 0;
  to_recv.msg_iov = &recv_vec;
  to_recv.msg_iovlen = 1;
  to_recv.msg_control = control_buf;
  to_recv.msg_controllen = CMSG_SPACE(sizeof(int));
  to_recv.msg_flags = 0;
  
  int ret = recvmsg(rcv_sock, &to_recv, 0);
  if(ret <= 0) {
    fprintf(stderr, "Failed to receive file descriptor: %s\n", strerror(errno));
    close(rcv_sock);
    return -1;
  }
  
  close(rcv_sock);
  
  buf[2] = '\0';
  if(strcmp(buf, "FD")) {
    fprintf(stderr, "Received inline data was not 'FD' (got %s)", buf);
    return -1;
  }
  
  if(to_recv.msg_controllen != CMSG_SPACE(sizeof(int))) {
    fprintf(stderr, "Didn't receive a file descriptor (control message length is set to %d, expected %d)\n", to_recv.msg_controllen, CMSG_SPACE(sizeof(int)));
    return -1;
  }
  
  struct cmsghdr* cmsg = CMSG_FIRSTHDR(&to_recv);
  int recvd_fd = *(int*)(CMSG_DATA(cmsg));

  printf("Received FD %d!\n", recvd_fd);

  return recvd_fd;

}

struct ciel_output {
  FILE* fp;
  int index;
  json_int_t bytes_written;
};

struct ciel_output* ciel_open_output(int index, int may_stream, int may_pipe, int make_sweetheart) {

  json_error_t error_bucket;

  json_t* open_output_message = json_pack_ex(&error_bucket, 0, "[s{sisbsbsbss}]", 
					     "open_output", "index", index,
					     "may_stream", may_stream, "may_pipe", may_pipe, 
					     "make_local_sweetheart", make_sweetheart,
					     "fd_socket_name", ciel_socket_name);

  if(!open_output_message)
    ciel_json_error(0, &error_bucket);
  
  ciel_write_framed_json(open_output_message, ciel_out);
  json_decref(open_output_message);

  json_t* open_response = ciel_read_framed_json(ciel_in);

  char* response_verb;
  json_t* response_args;

  if(json_unpack_ex(open_response, &error_bucket, 0, "[so]", &response_verb, &response_args))
    ciel_json_error(0, &error_bucket);

  if(strcmp(response_verb, "open_output") != 0) {
    fprintf(stderr, "open_output: bad response: %s\n", response_verb);
    exit(1);
  }

  json_t* sending_fd_json = json_object_get(response_args, "sending_fd");
  if(!sending_fd_json) {
    fprintf(stderr, "Open output response doesn't have member 'sending_fd'\n");
    json_decref(open_response);
    return 0;
  }

  struct ciel_output* ret = (struct ciel_output*)malloc(sizeof(struct ciel_output));
  ret->index = index;
  ret->bytes_written = 0;

  if(json_typeof(sending_fd_json) == JSON_TRUE) {

    int fd = ciel_receive_fd();
    ret->fp = fdopen(fd, "w");
    
  }
  else {

    json_t* filename_json = json_object_get(response_args, "filename");
    ret->fp = fopen(json_string_value(filename_json), "w");

  }

  json_decref(open_response);
  return ret;

}

void ciel_set_output_unbuffered(struct ciel_output* out) {

  setbuf(out->fp, 0);

}

int ciel_write_output(struct ciel_output* out, char* buf, int len) {

  int ret = fwrite(buf, 1, len, out->fp);
  if(ret >= 1) {
    out->bytes_written += ret;
  }

  return ret;

}

json_t* ciel_close_output(struct ciel_output* out) {

  json_error_t error_bucket;

  fflush(out->fp);
  fclose(out->fp);

  json_t* close_output_message = json_pack_ex(&error_bucket, 0, "[s{sisI}]", "close_output", "index", out->index, "size", out->bytes_written);
  if(!close_output_message)
    ciel_json_error(0, &error_bucket);
  ciel_write_framed_json(close_output_message, ciel_out);
  json_decref(close_output_message);

  free(out);

  json_t* response = ciel_read_framed_json(ciel_in);

  char* response_verb;
  json_t* ref;

  if(json_unpack_ex(response, &error_bucket, 0, "[s{sO}]", &response_verb, "ref", &ref))
    ciel_json_error(0, &error_bucket);

  if(strcmp(response_verb, "close_output") != 0) {
    fprintf(stderr, "close_output: bad response: %s\n", response_verb);
    exit(1);
  }

  json_decref(response);

  return ref;

}

void ciel_exit() {

  json_error_t error_bucket;

  json_t* exit_message = json_pack_ex(&error_bucket, 0, "[s{ss}]", "exit", "keep_process", "no");
  if(!exit_message)
    ciel_json_error(0, &error_bucket);
  ciel_write_framed_json(exit_message, ciel_out);

}

void ciel_define_output_with_plain_string(int index, char* string) {

  struct ciel_output* out = ciel_open_output(index, 0, 0, 0);
  int length = strlen(string);
  ciel_write_all(out->fp, string, length);
  out->bytes_written += length;
  json_t* ref = ciel_close_output(out);
  json_decref(ref);

}

json_t* ciel_get_task() {

  json_error_t error_bucket;
  json_t* task = ciel_read_framed_json(ciel_in);
  
  char* command;
  json_t* args;

  if(json_unpack_ex(task, &error_bucket, 0, "[sO]", &command, &args))
    ciel_json_error(0, &error_bucket);
  if(strcmp(command, "start_task") != 0) {
    fprintf(stderr, "Strange first task: %s\n", command);
    exit(1);
  }

  json_decref(task);
  return args;

}

// vargs: borrowed references
void ciel_block_on_refs(int n_refs, ...) {

  json_error_t error_bucket;
  va_list args;
  va_start(args, n_refs);

  json_t* ref_array = json_array();

  for(int i = 0; i < n_refs; i++) {
    json_array_append(ref_array, va_arg(args, json_t*));
  }

  va_end(args);

  json_t* tail_spawn_message = json_pack_ex(&error_bucket, 0, "[s{sssosb}]", 
					    "tail_spawn", "executor_name", "proc", 
					    "extra_dependencies", ref_array, "is_fixed", 1);
  if(!tail_spawn_message)
    ciel_json_error(0, &error_bucket);

  ciel_write_framed_json(tail_spawn_message, ciel_out);

  json_decref(tail_spawn_message);

  json_t* exit_message = json_pack_ex(&error_bucket, 0, "[s{ss}]", "exit", "keep_process", "must_keep");
  if(!exit_message)
    ciel_json_error(0, &error_bucket);

  ciel_write_framed_json(exit_message, ciel_out);

  json_decref(exit_message);

  json_t* task_private = ciel_get_task();
  // Don't care
  json_decref(task_private);

}

struct ciel_input {

  FILE* fp;
  int chunk_size;
  json_int_t bytes_read;
  int eof;
  int is_blocking;
  int must_close;
  char* refid;

};

// Returns NEW char*; caller must free
char* ciel_get_ref_id(json_t* ref) {

  // Extract ref's ID from JSON: a dangerous assumption: the ref's ID is always in position 1 (see references.py::build_reference_from_tuple)

  // Both these functions borrow references from their container.
  json_t* ref_tuple = json_object_get(ref, "__ref__");
  json_t* ref_id = json_array_get(ref_tuple, 1);

  // Dup borrowed string
  return strdup(json_string_value(ref_id));

}

// ref: borrowed reference
struct ciel_input* ciel_open_ref_async(json_t* ref, int chunk_size, int sole_consumer, int must_block) {

  json_error_t error_bucket;

  json_t* open_message = json_pack_ex(&error_bucket, 0, "[s{sOsisbsbsbss}]", "open_ref_async",
				      "ref", ref, "chunk_size", chunk_size, "sole_consumer", sole_consumer,
				      "make_sweetheart", 0, "must_block", must_block, "fd_socket_name", ciel_socket_name);
  if(!open_message)
    ciel_json_error(0, &error_bucket);

  ciel_write_framed_json(open_message, ciel_out);
  json_decref(open_message);

  json_t* response = ciel_read_framed_json(ciel_in);

  char* response_verb;
  json_t* response_args;

  if(json_unpack_ex(response, &error_bucket, 0, "[so]", &response_verb, &response_args)) {
    fprintf(stderr, "Failed decoding open_ref_async array\n");
    ciel_json_error(0, &error_bucket);
    json_decref(response);
    return 0;
  }

  int fd_coming;
  int is_blocking;
  int is_done;
  int size;

  if(json_unpack_ex(response_args, &error_bucket, 0, "{sbsbsb}", 
		    "sending_fd", &fd_coming, "blocking", &is_blocking, 
		    "done", &is_done)) {
    fprintf(stderr, "Failed decoding open_ref_async object\n");
    ciel_json_error(0, &error_bucket);
    json_decref(response);
    return 0;
  }

  struct ciel_input* new_input = (struct ciel_input*)malloc(sizeof(struct ciel_input));

  new_input->chunk_size = chunk_size;
  new_input->is_blocking = is_blocking;
  new_input->must_close = !is_done;
  new_input->eof = is_done;
  new_input->bytes_read = 0;

  if(!fd_coming) {
    json_t* filename_obj = json_object_get(response_args, "filename");
    if(!filename_obj) {
      fprintf(stderr, "No member 'filename' in open_ref_async response\n");
      json_decref(response);
      return 0;
    }
    const char* filename = json_string_value(filename_obj);
    if(!filename) {
      fprintf(stderr, "Member 'filename' in open_ref_async not a string\n");
      json_decref(response);
      return 0;
    }
    new_input->fp = fopen(filename, "r");
  }
  else {
    
    int recvd_fd = ciel_receive_fd();
    if(recvd_fd == -1) {
      fprintf(stderr, "Receive-FD failed in open_ref_async\n");
      json_decref(response);
      return 0;
    }
    new_input->fp = fdopen(recvd_fd, "r");
    
  }

  json_decref(response);

  new_input->refid = ciel_get_ref_id(ref);

  return new_input;

}

struct ciel_input* ciel_open_ref(json_t* ref) {

  json_error_t error_bucket;
  
  json_t* open_message = json_pack_ex(&error_bucket, 0, "[s{sO}]", "open_ref", "ref", ref);
  if(!open_message) {
    ciel_json_error(0, &error_bucket);
    return 0;
  }

  ciel_write_framed_json(open_message, ciel_out);
  json_decref(open_message);

  char* response_verb;
  char* filename;

  json_t* response = ciel_read_framed_json(ciel_in);
  if(json_unpack_ex(response, &error_bucket, 0, "[s{ss}]", &response_verb, "filename", &filename)) {
    ciel_json_error(0, &error_bucket);
    return 0;
  }

  struct ciel_input* ret = (struct ciel_input*)malloc(sizeof(struct ciel_input));

  ret->fp = fopen(filename, "r");
  ret->chunk_size = 0;
  ret->is_blocking = 1;
  ret->must_close = 0;
  ret->bytes_read = 0;
  ret->eof = 0;
  ret->refid = ciel_get_ref_id(ref);

  json_decref(response);

  return ret;

}

void ciel_set_input_unbuffered(struct ciel_input* ref) {

  setbuf(ref->fp, 0);

}

int ciel_read_ref(struct ciel_input* ref, char* buffer, int length) {

  json_error_t error_bucket;

  while(1) {
    size_t bytes_read = fread(buffer, 1, length, ref->fp);
    if(bytes_read == 0) {
      if(ferror(ref->fp)) {
	fprintf(stderr, "Error reading ref id %s\n", ref->refid);
	return -1;
      }
      else {
	if(ref->eof || ref->is_blocking) {
	  return 0;
	}
	else {
	  json_int_t threshold = ref->bytes_read + ref->chunk_size;
	  json_t* wait_message = json_pack_ex(&error_bucket, 0, "[s{sssI}]", "wait_stream", "id", ref->refid, "bytes", threshold);
	  ciel_write_framed_json(wait_message, ciel_out);
	  json_decref(wait_message);
	  
	  json_t* response = ciel_read_framed_json(ciel_in);

	  char* response_verb;
	  int new_size;
	  int new_done;
	  int success;

	  if(json_unpack_ex(response, &error_bucket, 0, "[s{sisbsb}]", &response_verb, "size", &new_size, "done", &new_done, "success", &success))
	    ciel_json_error(0, &error_bucket);

	  ref->eof = new_done;

	  if(strcmp(response_verb, "wait_stream")) {
	    fprintf(stderr, "Weird response to wait_stream: %s\n", response_verb);
	    json_decref(response);
	    return -1;
	  }
	  if(!success) {
	    fprintf(stderr, "Error waiting for ref %s!\n", ref->refid);
	    json_decref(response);
	    return -1;
	  }

	  json_decref(response);
	}
      }
    }
    else {
      ref->bytes_read += bytes_read;
      return bytes_read;
    }
  }
}

void ciel_close_ref(struct ciel_input* ref) {

  json_error_t error_bucket;

  fflush(ref->fp);
  fclose(ref->fp);
  
  if(ref->must_close) {
    json_t* close_message = json_pack_ex(&error_bucket, 0, "[s{sssi}]", "close_stream", "id", ref->refid, "chunk_size", ref->chunk_size);
    if(!close_message)
      ciel_json_error(0, &error_bucket);
    ciel_write_framed_json(close_message, ciel_out);
    json_decref(close_message);
  }

  free(ref->refid);
  free(ref);

}

void ciel_init(char* out_fname, char* in_fname) {

  ciel_out = fopen(out_fname, "w");
  ciel_in = fopen(in_fname, "r");
  if((!ciel_in) || (!ciel_out)) {
    fprintf(stderr, "Couldn't open either %s or %s\n", out_fname, in_fname);
    exit(1);
  }

  char socket_name[] = "/tmp/ciel_fdsockXXXXXX";
  char* real_name = mktemp(socket_name);
  if(!real_name) {
    fprintf(stderr, "Couldn't mktemp\n");
    exit(1);
  }
  int sock = socket(AF_UNIX, SOCK_STREAM, 0);
  if(sock == -1) {
    fprintf(stderr, "Couldn't create socket\n");
    exit(1);
  }
  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(struct sockaddr_un));
  addr.sun_family = AF_UNIX;
  strcpy(addr.sun_path, real_name);
  
  int ret = bind(sock, (struct sockaddr*)&addr, sizeof(struct sockaddr_un));
  if(ret == -1) {
    fprintf(stderr, "Couldn't bind\n");
    exit(1);
  }

  ret = listen(sock, 5);
  if(ret == -1) {
    fprintf(stderr, "Couldn't listen\n");
    exit(1);
  }

  ciel_socket_fd = sock;
  ciel_socket_name = strdup(real_name);

}
