add_executable(client
        main.c
        main.h
        2_phase_1_sided.c
        2_phase_1_sided.h
        put.h
        put.c
        request_region_connection.c
        request_region_connection.h
        ack_region.c
        ack_region.h
        profiler_metrics.h
        profiler_report_layout.c
        profiler_report_layout.h
        2_phase_2_sided.c
        2_phase_2_sided.h
        phase_2_queue.c
        phase_2_queue.h)

target_link_libraries(client PRIVATE commonLib)
target_link_libraries(client PRIVATE SISCI)
target_link_libraries(client PRIVATE m)
