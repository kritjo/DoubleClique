find_library(SISCI_LIB
        NAMES sisci
        PATHS /opt/DIS/lib64
        NO_DEFAULT_PATH
        REQUIRED
)

if(SISCI_LIB)
    # Create an imported library target
    add_library(SISCI STATIC IMPORTED)

    set_target_properties(SISCI PROPERTIES
            IMPORTED_LOCATION "${SISCI_LIB}"
            INTERFACE_INCLUDE_DIRECTORIES "/opt/DIS/include"
    )

    # Indicate to the parent that we found SISCI
    set(SISCI_FOUND TRUE)
else()
    message(FATAL_ERROR "Could not find the SISCI library in /opt/DIS/lib64.")
endif()
