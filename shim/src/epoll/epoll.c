// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "../epoll.h"
#include "../log.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct epoll_table
{
    int used;
    struct demi_event events[MAX_EVENTS];
};

static struct epoll_table global_epoll_table[EPOLL_MAX_FDS];
static __thread struct epoll_table *worker_epoll_table;
static __thread struct epoll_table *epoll_table = 0;

int epoll_table_alloc(void)
{
    for (int i = 0; i < EPOLL_MAX_FDS; i++)
    {
        if (epoll_table[i].used == 0)
        {
            epoll_table[i].used = 1;
            return i;
        }
    }

    return -1;
}

void epoll_table_init_worker(void)
{
    if (worker_epoll_table == 0) {
        worker_epoll_table = malloc(EPOLL_MAX_FDS * sizeof(struct epoll_table));
        if (worker_epoll_table == NULL) {
            exit(-1);
        }
        memset(worker_epoll_table, 0, EPOLL_MAX_FDS * sizeof(struct epoll_table));
    }

    if (epoll_table == 0) {
        epoll_table = worker_epoll_table;
    }

    for (int i = 0; i < EPOLL_MAX_FDS; i++)
    {
        epoll_table[i].used = 0;
        for (int j = 0; j < MAX_EVENTS; j++)
            epoll_table[i].events[j].used = 0;
    }
}

void epoll_table_init(void)
{
    if (epoll_table == 0) {
        epoll_table = global_epoll_table;
    }
    for (int i = 0; i < EPOLL_MAX_FDS; i++)
    {
        epoll_table[i].used = 0;
        for (int j = 0; j < MAX_EVENTS; j++)
            epoll_table[i].events[j].used = 0;
    }
}

struct demi_event *epoll_get_event(int epfd, int i)
{
    assert(epoll_table[epfd].used == 1);
    return (&epoll_table[epfd].events[i]);
}
