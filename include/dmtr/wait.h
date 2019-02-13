#ifndef DMTR_WAIT_H_IS_INCLUDED
#define DMTR_WAIT_H_IS_INCLUDED

#include <dmtr/sys/gcc.h>
#include <dmtr/types.h>

#ifdef __cplusplus
extern "C" {
#endif

DMTR_EXPORT int dmtr_wait(dmtr_sgarray_t *sga_out, dmtr_qtoken_t qt);
DMTR_EXPORT int dmtr_wait_any(dmtr_wait_completion_t *wait_out, dmtr_qtoken_t qts[], int num_qts);
                              
                              
#ifdef __cplusplus
}
#endif

#endif /* DMTR_WAIT_H_IS_INCLUDED */
