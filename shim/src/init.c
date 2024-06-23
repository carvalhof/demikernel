// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "epoll.h"
#include "log.h"
#include "qman.h"
#include "utils.h"
#include <demi/libos.h>
#include <errno.h>
#include <glue.h>

#include <pthread.h>

int __init(int nr_workers)
{
    int ret = -1;
    int argc = 2;

    char lcores_str[10];
    sprintf(lcores_str, "%d", nr_workers);

    char *const argv[] = {"shim", lcores_str};

    // TODO: Pass down arguments correctly.
    TRACE("argc=%d argv={%s}", argc, argv[0]);
    if(nr_workers == 0) {
        queue_man_init_worker();
        epoll_table_init_worker();
    } else {
        queue_man_init();
        epoll_table_init();
    }
    
    // queue_man_init();
    // epoll_table_init();

    ret = __demi_init(argc, argv);

    if (ret != 0)
    {
        errno = ret;
        return -1;
    }

    return 0;
}

int __init_workers(int nr_workers)
{
    int ret = -1;
    int argc = 2;

    char lcores_str[10];
    sprintf(lcores_str, "%d", nr_workers);

    char *const argv[] = {"shim", lcores_str};

    // TODO: Pass down arguments correctly.
    TRACE("argc=%d argv={%s}", argc, argv[0]);

    queue_man_init_worker();
    epoll_table_init_worker();

    const struct demi_args args = {
        .argc = argc,
        .argv = argv,
        .callback = NULL,
    };
    ret = __demi_init(NULL);

    if (ret != 0)
    {
        errno = ret;
        return -1;
    }

    return 0;
}
