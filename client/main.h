#ifndef DOUBLECLIQUE_MAIN_H
#define DOUBLECLIQUE_MAIN_H

#include <stdint.h>
#include <sisci_types.h>
#include <pthread.h>
#include "put_request_region.h"
#include "index_data_protocol.h"
#include "sync_point.h"

extern sync_point_t put_sync_point;

int main(int argc, char* argv[]);

#endif //DOUBLECLIQUE_MAIN_H
