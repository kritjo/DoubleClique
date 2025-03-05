#ifndef DOUBLECLIQUE_MAIN_H
#define DOUBLECLIQUE_MAIN_H

#include <stdint.h>
#include <sisci_types.h>
#include <pthread.h>
#include "request_region.h"
#include "index_data_protocol.h"

enum get_return_status {
    GET_RETURN_SUCCESS,
    GET_RETURN_ERROR
};

typedef struct {
    enum get_return_status status;
    uint32_t data_length;
    void *data;
    const char *error_message;
} get_return_t;

int main(int argc, char* argv[]);

#endif //DOUBLECLIQUE_MAIN_H
