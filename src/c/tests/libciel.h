
#include <jansson.h>

void ciel_json_error(char* string, json_error_t* error);

void ciel_read_all(FILE* fin, int length, char* dest);

void ciel_write_all(FILE* fout, char* buf, int len);

char* ciel_open_output(int index, int may_stream, int may_pipe, int make_sweetheart);

json_t* ciel_close_output(int index, int size);

void ciel_exit();

void ciel_define_output_with_plain_string(int index, char* string);

void ciel_init(char* out_fname, char* in_fname);

json_t* ciel_get_task();

struct ciel_input;

void ciel_close_ref(struct ciel_input* ref);

int ciel_read_ref(struct ciel_input* ref, char* buffer, int length);

struct ciel_input* ciel_open_ref_async(json_t* ref, int chunk_size, int may_stream, int sole_consumer, int must_block);

void ciel_block_on_refs(int n_refs, ...);
