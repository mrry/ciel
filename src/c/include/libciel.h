
#include <jansson.h>

void ciel_json_error(char* string, json_error_t* error);

void ciel_read_all(FILE* fin, int length, char* dest);

void ciel_write_all(FILE* fout, char* buf, int len);

struct ciel_output;

struct ciel_output* ciel_open_output(int index, int may_stream, int may_pipe, int make_sweetheart);

int ciel_write_output(struct ciel_output*, char* buf, int len);

json_t* ciel_close_output(struct ciel_output*);

void ciel_set_output_unbuffered(struct ciel_output* out);

void ciel_exit();

void ciel_define_output_with_plain_string(int index, char* string);

void ciel_init(char* out_fname, char* in_fname);

json_t* ciel_get_task();

struct ciel_input;

void ciel_close_ref(struct ciel_input* ref);

void ciel_set_input_unbuffered(struct ciel_input* in);

int ciel_read_ref(struct ciel_input* ref, char* buffer, int length);

struct ciel_input* ciel_open_ref(json_t* ref);

struct ciel_input* ciel_open_ref_async(json_t* ref, int chunk_size, int sole_consumer, int must_block);

void ciel_block_on_refs(int n_refs, ...);
