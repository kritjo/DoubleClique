#include "main.h"

#include <stdlib.h>
#include <sisci_api.h>

#include "sisci_glob_defs.h"

int main(int argc, char* argv[]) {
    sci_desc_t sd;

    SEOE(SCIInitialize, NO_FLAGS);
    SEOE(SCIOpen, &sd, NO_FLAGS);



    SEOE(SCIClose, sd, NO_FLAGS);
    SCITerminate();

    return EXIT_SUCCESS;
}