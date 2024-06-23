// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

#include "../epoll.h"
#include "../error.h"
#include "../log.h"
#include "../qman.h"
#include "../utils.h"
#include <assert.h>
#include <demi/libos.h>
#include <demi/sga.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <glue.h>

static __thread char real_buffer[2048];
static __thread char *buffered;
static __thread int buffered_len;
static __thread demi_sgarray_t *buffered_sga;

static size_t fill_iov(const struct iovec *iov, demi_sgarray_t *sga,
        uint32_t iovcnt, uint32_t sgacnt);

ssize_t __read(int sockfd, void *buf, size_t count)
{
    int epfd = -1;

    // Check if this socket descriptor is managed by Demikernel.
    // If that is not the case, then fail to let the Linux kernel handle it.
    if (!queue_man_query_fd(sockfd))
    {
        errno = EBADF;
        return (-1);
    }

    // Check if socket descriptor is registered on an epoll instance.
    if ((epfd = queue_man_query_fd_pollable(sockfd)) > 0)
    {
        TRACE("sockfd=%d, buf=%p, count=%zu", sockfd, buf, count);
        struct demi_event *ev = NULL;

        // Check if read operation has completed.
        if ((ev = queue_man_get_pop_result(sockfd)) != NULL)
        {
            int last_idx = 0;
            int total_copied = 0;

            // printf("\n\n\n EV-QT = %d\n\n\n", ev->qt);

            if(ev->qt != 1) {
                // Isso eh uma nova solicitacao provavelmente do restante do buiffer
                if(buffered_len != 0) {
                    // printf("\n[MENSAGEM--BUFFER]\n\n");
                    // for(int i= 0; i < buffered_len; i++) {
                    //     printf("%c", ((char*) buffered)[i]);
                    // }
                    // printf("\n\n[%d]\n\n\n", buffered_len);

                    if(count < buffered_len) {
                        memcpy(buf, buffered, count);
                        buffered = buffered + count;
                        buffered_len = buffered_len - count;

                        return (count);
                    } else {
                        // printf("\n\t\t\t eh o caso em que o que a a aplicara quer eh mais do que tem no buffer\n");
                        // printf("\t\t\t buffered_len=%d e precciso pegar count-buffered_len=%ld bytes\n", buffered_len, count-buffered_len);
                        last_idx = buffered_len;
                        memcpy(buf, buffered, buffered_len);
                        total_copied += buffered_len;
                        // atualiza o count
                        // printf("\t\t\t atualizando o count de %ld para %ld\n", count, count-buffered_len);
                        count = count - buffered_len;
                        // buffered = real_buffer;
                        buffered_len = 0;
                        // memset(real_buffer, 0, 2048);
                        // __demi_sgafree(buffered_sga);

                        // printf("\n[MENSAGEM--APLICACAO]\n\n");
                        // for(int i= 0; i < total_copied; i++) {
                        //     printf("%c", ((char*) buf)[i]);
                        // }
                        // printf("\n\n[%d]\n\n\n", total_copied);

                        // printf("\n\t\t\t\t COUNT era %ld e TOTAL_COPIED era %d\n", count, total_copied);
                        return (total_copied);
                    }
                }
            }

            // // se tem buffer, eh dessa ultima mensagem, então podemos aumentar
            // if(ev->qt != -1 && buffered_len != 0) {
            //     ev->qr.qr_value.sga.sga_segs[0].sgaseg_buf = ((char*)ev->qr.qr_value.sga.sga_segs[0].sgaseg_buf) + buffered_len;
            //     ev->qr.qr_value.sga.sga_segs[0].sgaseg_len -= buffered_len;
            // }

            // printf("\n[MENSAGEM] -- do eve\n\n");
            // for(int i= 0; i < ev->qr.qr_value.sga.sga_segs[0].sgaseg_len; i++) {
            //     printf("%c", ((char*) ev->qr.qr_value.sga.sga_segs[0].sgaseg_buf)[i]);
            // }
            // printf("\n\n[%d]\n\n\n", ev->qr.qr_value.sga.sga_segs[0].sgaseg_len);

            // int last_idx = 0;
            // int total_copied = 0;
            if(buffered_len != 0) {
            //     printf("\n[MENSAGEM--BUFFER]\n\n");
            // for(int i= 0; i < buffered_len; i++) {
            //     printf("%c", ((char*) buffered)[i]);
            // }
            // printf("\n\n[%d]\n\n\n", buffered_len);


                if(count < buffered_len) {
                    memcpy(buf, buffered, count);
                    buffered = buffered + count;
                    buffered_len = buffered_len - count;

                    // printf("\t\t\tESTOU PEGANDO %ld e tem %d bytes\n", count, buffered_len);

                    return (count);
                } else {
                    // printf("\n\t\t\t eh o caso em que o que a a aplicara quer eh mais do que tem no buffer\n");
                    // printf("\t\t\t buffered_len=%d e precciso pegar count-buffered_len=%ld bytes\n", buffered_len, count-buffered_len);
                    last_idx = buffered_len;
                    memcpy(buf, buffered, buffered_len);
                    total_copied += buffered_len;
                    // atualiza o count
                    // printf("\t\t\t atualizando o count de %ld para %ld\n", count, count-buffered_len);
                    count = count - buffered_len;
                    // buffered = real_buffer;
                    buffered_len = 0;
                    // memset(real_buffer, 0, 2048);
                    // __demi_sgafree(buffered_sga);
                }
            } else {
                // printf("\n\t\t\t esse eh o caso em que nao tem nada bufferizado e quero pegar %ld\n", count);
            }

            assert(ev->used == 1);
            assert(ev->sockqd == sockfd);
            // assert(ev->qt == (demi_qtoken_t)-1);
            assert(ev->qr.qr_value.sga.sga_numsegs == 1);

            // TODO: We should support buffering.
            if (count < ev->qr.qr_value.sga.sga_segs[0].sgaseg_len) {
                // |-----------sgaseg_len------------|
                // |----count----|----remaining------|

                // printf("\t\t\t caso em que quero buscar algo menorr que recebi len=%d e count=%ld\n", ev->qr.qr_value.sga.sga_segs[0].sgaseg_len, count);

                buffered_sga = &ev->qr.qr_value.sga;
                buffered_len = ev->qr.qr_value.sga.sga_segs[0].sgaseg_len - count;
                // memcpy(real_buffer, ((char*) ev->qr.qr_value.sga.sga_segs[0].sgaseg_buf) + count, buffered_len);
                buffered = ((char*) ev->qr.qr_value.sga.sga_segs[0].sgaseg_buf) + count;
                // buffered = real_buffer;

                //test


                // printf("\t\t\t com isso, bufferizei %d bytes\n", buffered_len);
                // printf("\t\t\t e marco o socket como pronto para outro pop\n");

                ev->qt = -2;
                queue_man_set_pop_result(ev->qr.qr_qd, ev);
                //tenho que marcar que eh de uma retransmissao...

            } else {
                // printf("\t\t\t caso em que tenho tudo que queria em que count =%ld e o buffer recebido eh %d\n", count, ev->qr.qr_value.sga.sga_segs[0].sgaseg_len);
                // Round down the number of bytes to read accordingly.
                count = MIN(count, ev->qr.qr_value.sga.sga_segs[0].sgaseg_len);
            }
            // assert(count >= ev->qr.qr_value.sga.sga_segs[0].sgaseg_len);

            // printf("\t\t\t no final das contas, eu quero count=%ld, buffered_len=%d e recebvido=%d e idx=%d\n", count, buffered_len, ev->qr.qr_value.sga.sga_segs[0].sgaseg_len, last_idx);

            // // Round down the number of bytes to read accordingly.
            // count = MIN(count, ev->qr.qr_value.sga.sga_segs[0].sgaseg_len);

            if (ev->qr.qr_value.sga.sga_segs[0].sgaseg_len == 0)
            {
                TRACE("read zero bytes");
                __demi_sgafree(&ev->qr.qr_value.sga);
                return (0);
            }

            if (count > 0)
            {
                memcpy((char*) buf + last_idx, ev->qr.qr_value.sga.sga_segs[0].sgaseg_buf, count);
                total_copied += count;
                __demi_sgafree(&ev->qr.qr_value.sga);
            }

            // Re-issue I/O queue operation.
            assert(__demi_pop(&ev->qt, ev->sockqd) == 0);
            assert(ev->qt != (demi_qtoken_t)-1);

            // return (count);

            // printf("\n[MENSAGEM--APLICACAO]\n\n");
            // for(int i= 0; i < total_copied; i++) {
            //     printf("%c", ((char*) buf)[i]);
            // }
            // printf("\n\n[%d]\n\n\n", total_copied);

            // printf("\n\t\t\t\t COUNT era %ld e TOTAL_COPIED era %d\n", count, total_copied);
            return (total_copied);
        }

        // The read operation has not yet completed.
        errno = EWOULDBLOCK;

        return (-1);
    }

    // TODO: Hook in demi_read().
    UNIMPLEMETED("read() currently works only on epoll mode");

    return (-1);
}

ssize_t __recv(int sockfd, void *buf, size_t len, int flags)
{
    // Check if this socket descriptor is managed by Demikernel.
    // If that is not the case, then fail to let the Linux kernel handle it.
    if (!queue_man_query_fd(sockfd))
    {
        errno = EBADF;
        return (-1);
    }

    TRACE("sockfd=%d, buf=%p, flags=%x", sockfd, buf, flags);

    // TODO: Hook in demi_recv().
    UNUSED(buf);
    UNUSED(len);
    UNUSED(flags);
    UNIMPLEMETED("recv() is not hooked in");

    return (-1);
}

ssize_t __recvfrom(int sockfd, void *buf, size_t len, int flags, struct sockaddr *src_addr, socklen_t *addrlen)
{
    // Check if this socket descriptor is managed by Demikernel.
    // If that is not the case, then fail to let the Linux kernel handle it.
    if (!queue_man_query_fd(sockfd))
    {
        errno = EBADF;
        return (-1);
    }

    TRACE("sockfd=%d, buf=%p, len=%zu, flags=%x, src_addr=%p, addrlen=%p", sockfd, buf, len, flags, (void *)src_addr,
          (void *)addrlen);

    // TODO: Hook in demi_recvfrom().
    UNUSED(buf);
    UNUSED(len);
    UNUSED(flags);
    UNUSED(src_addr);
    UNUSED(addrlen);
    UNIMPLEMETED("recevfrom() is not hooked in");

    return (-1);
}

ssize_t __recvmsg(int sockfd, struct msghdr *msg, int flags)
{
    // Check if this socket descriptor is managed by Demikernel.
    // If that is not the case, then fail to let the Linux kernel handle it.
    if (!queue_man_query_fd(sockfd))
    {
        errno = EBADF;
        return (-1);
    }

    TRACE("sockfd=%d, msg=%p, flags=%x", sockfd, (void *)msg, flags);

    // TODO: Hook in demi_recvmsg().
    UNUSED(msg);
    UNUSED(flags);
    UNIMPLEMETED("recvmsg() is not hooked in");

    return (-1);
}

ssize_t __readv(int sockfd, const struct iovec *iov, int iovcnt)
{
    size_t count;
    uint32_t num_segs;
    int epfd = -1;

    if (iovcnt < 0)
    {
        errno = EINVAL;
        return (-1);
    }

    // Check if this socket descriptor is managed by Demikernel.
    // If that is not the case, then fail to let the Linux kernel handle it.
    if (!queue_man_query_fd(sockfd))
    {
        errno = EBADF;
        return (-1);
    }

    // Check if socket descriptor is registered on an epoll instance.
    if ((epfd = queue_man_query_fd_pollable(sockfd)) > 0)
    {
        TRACE("sockfd=%d, iov=%p, iovcnt=%zu", sockfd, iov, iovcnt);
        struct demi_event *ev = NULL;

        // Check if read operation has completed.
        if ((ev = queue_man_get_pop_result(sockfd)) != NULL)
        {
            assert(ev->used == 1);
            assert(ev->sockqd == sockfd);
            assert(ev->qt == (demi_qtoken_t)-1);

            num_segs = ev->qr.qr_value.sga.sga_numsegs;

            if (ev->qr.qr_value.sga.sga_segs[0].sgaseg_len == 0)
            {
                TRACE("read zero bytes");
                __demi_sgafree(&ev->qr.qr_value.sga);
                return (0);
            }

            count = fill_iov(iov, &ev->qr.qr_value.sga, iovcnt, num_segs);
            if (count > 0)
            {
                __demi_sgafree(&ev->qr.qr_value.sga);
            }

            // Re-issue I/O queue operation.
            assert(__demi_pop(&ev->qt, ev->sockqd) == 0);
            assert(ev->qt != (demi_qtoken_t)-1);

            return (count);
        }

        // The read operation has not yet completed.
        errno = EWOULDBLOCK;
        return (-1);
    }

    UNIMPLEMETED("read() currently works only on epoll mode");

    return (-1);
}

ssize_t __pread(int sockfd, void *buf, size_t count, off_t offset)
{
    // Check if this socket descriptor is managed by Demikernel.
    // If that is not the case, then fail to let the Linux kernel handle it.
    if (!queue_man_query_fd(sockfd))
    {
        errno = EBADF;
        return (-1);
    }

    TRACE("sockfd=%d, buf=%p, count=%zu, off=%ld", sockfd, buf, count, offset);

    // TODO: Hook in demi_pread().
    UNUSED(buf);
    UNUSED(count);
    UNUSED(offset);
    UNIMPLEMETED("pread() is not hooked in");

    return (-1);
}

static size_t fill_iov(const struct iovec *iov, demi_sgarray_t *sga,
        uint32_t iovcnt, uint32_t sgacnt)
{
    ssize_t count, total;
    size_t iov_len, sga_len;
    uint32_t i_iov = 0, i_sga = 0;
    uint8_t iov_off = 0, sga_off = 0;


    total = 0;
    while (i_iov < iovcnt && i_sga < sgacnt)
    {
        iov_len = iov[i_iov].iov_len - iov_off;
        sga_len = sga->sga_segs[i_sga].sgaseg_len - sga_off;
        count = MIN(iov_len, sga_len);
        memcpy(iov[i_iov].iov_base + iov_off,
                sga->sga_segs[i_sga].sgaseg_buf + sga_off, count);

        total += count;

        if (iov_len > sga_len)
        {
            i_sga += 1;
            iov_off = count;
            sga_off = 0;
        }
        else if (iov_len < sga_len)
        {
            i_iov += 1;
            iov_off = 0;
            sga_off = count;
        }
        else if (iov_len == sga_len)
        {
            i_iov += 1;
            i_sga += 1;
            iov_off = 0;
            sga_off = 0;
        }
    }

    // TODO: We should support buffering
    assert(i_iov  < iovcnt || i_sga == sgacnt);

    return (total);
}
