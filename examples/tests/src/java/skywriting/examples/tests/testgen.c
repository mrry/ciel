#include <unistd.h>
#include <string.h>

int main() {

  const char test_string[] = "2,2,2\0/tmp/test1\0/tmp/test2\0/tmp/test3\0/tmp/test4\0arg1\0arg2";

  write(1, test_string, sizeof(test_string));

}
