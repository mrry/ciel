
#define _GNU_SOURCE

#include <jansson.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <arpa/inet.h>

FILE* ciel_out;
FILE* ciel_in;

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

char* ciel_open_output(int index, int may_stream, int may_pipe, int make_sweetheart) {

  json_error_t error_bucket;

  json_t* open_output_message = json_pack_ex(&error_bucket, 0, "[s{sisbsbsb}]", 
					     "open_output", "index", index,
					     "may_stream", may_stream, "may_pipe", may_pipe, 
					     "make_local_sweetheart", make_sweetheart);

  if(!open_output_message)
    ciel_json_error(0, &error_bucket);
  
  ciel_write_framed_json(open_output_message, ciel_out);
  json_decref(open_output_message);

  json_t* open_response = ciel_read_framed_json(ciel_in);

  char* response_verb;
  char* filename;

  if(json_unpack_ex(open_response, &error_bucket, 0, "[s{ss}]", &response_verb, "filename", &filename))
    ciel_json_error(0, &error_bucket);

  if(strcmp(response_verb, "open_output") != 0) {
    fprintf(stderr, "open_output: bad response: %s\n", response_verb);
    exit(1);
  }

  char* ret = strdup(filename);
  json_decref(open_response);
  return ret;

}

json_t* ciel_close_output(int index, int size) {

  json_error_t error_bucket;

  json_t* close_output_message = json_pack_ex(&error_bucket, 0, "[s{sisi}]", "close_output", "index", index, "size", size);
  if(!close_output_message)
    ciel_json_error(0, &error_bucket);
  ciel_write_framed_json(close_output_message, ciel_out);
  json_decref(close_output_message);

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

  char* filename = ciel_open_output(index, 0, 0, 0);
  FILE* fp = fopen(filename, "w");
  int length = strlen(string);
  ciel_write_all(fp, string, length);
  fflush(fp);
  fclose(fp);
  json_t* ref = ciel_close_output(index, length);
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
  int bytes_read;
  int eof;
  int is_blocking;
  int must_close;
  char* refid;

};

// ref: borrowed reference
struct ciel_input* ciel_open_ref_async(json_t* ref, int chunk_size, int may_stream, int sole_consumer, int must_block) {

  json_error_t error_bucket;

  json_t* open_message = json_pack_ex(&error_bucket, 0, "[s{sOsisbsbsb}]", "open_ref_async",
				      "ref", ref, "chunk_size", chunk_size, "sole_consumer", sole_consumer,
				      "make_sweetheart", 0, "must_block", must_block);
  if(!open_message)
    ciel_json_error(0, &error_bucket);

  ciel_write_framed_json(open_message, ciel_out);
  json_decref(open_message);

  json_t* response = ciel_read_framed_json(ciel_in);

  char* response_verb;
  char* filename;
  int is_blocking;
  int is_done;
  int size;

  if(json_unpack_ex(response, &error_bucket, 0, "[s{sssbsbsi}]", &response_verb, 
		    "filename", &filename, "blocking", &is_blocking, 
		    "done", &is_done, "size", &size))
    ciel_json_error(0, &error_bucket);

  struct ciel_input* new_input = (struct ciel_input*)malloc(sizeof(struct ciel_input));

  new_input->fp = fopen(filename, "r");
  new_input->chunk_size = chunk_size;
  new_input->is_blocking = is_blocking;
  new_input->must_close = !is_done;
  new_input->eof = is_done;

  json_decref(response);

  // Extract ref's ID from JSON: a dangerous assumption: the ref's ID is always in position 1 (see references.py::build_reference_from_tuple)

  // Both these functions borrow references from their container.
  json_t* ref_tuple = json_object_get(ref, "__ref__");
  json_t* ref_id = json_array_get(ref_tuple, 1);

  // Dup borrowed string
  new_input->refid = strdup(json_string_value(ref_id));

  return new_input;

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
	  int threshold = ref->bytes_read + ref->chunk_size;
	  json_t* wait_message = json_pack_ex(&error_bucket, 0, "[s{sssi}]", "wait_stream", "id", ref->refid, "bytes", threshold);
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

}
