/* Copyright (c) 2010-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright
 * notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
 * RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
 * CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
 * CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#ifndef RAMCLOUD_SYSCALL_H
#define RAMCLOUD_SYSCALL_H

#include <linux/futex.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <cstdio>

#include "Common.h"

namespace RAMCloud {

/**
 * This class provides a mechanism for invoking system calls and other
 * library functions that makes it easy to intercept the calls with a
 * mock class (e.g. MockSyscall) for testing. When system calls are
 * invoked through this base class they have the same behavior as if
 * they were invoked directly.
 *
 * The methods have the same names, arguments, and behavior as the
 * corresponding Linux/POSIX functions; see the man pages for details.
 */
class Syscall {
    public:
    Syscall() {}
    VIRTUAL_FOR_TESTING ~Syscall() {}

    VIRTUAL_FOR_TESTING
    int accept(int sockfd, sockaddr *addr, socklen_t *addrlen) {
        return ::accept(sockfd, addr, addrlen);
    }
    VIRTUAL_FOR_TESTING
    int bind(int sockfd, const sockaddr *addr, socklen_t addrlen) {
        return ::bind(sockfd, addr, addrlen);
    }
    VIRTUAL_FOR_TESTING
    int close(int fd) {
        return ::close(fd);
    }
    VIRTUAL_FOR_TESTING
    int connect(int sockfd, const sockaddr *addr,
                socklen_t addrlen) {
        return ::connect(sockfd, addr, addrlen);
    }
    VIRTUAL_FOR_TESTING
    int epoll_create(int size) {
        return ::epoll_create(size);
    }
    VIRTUAL_FOR_TESTING
    int epoll_ctl(int epfd, int op, int fd, epoll_event *event) {
        return ::epoll_ctl(epfd, op, fd, event);
    }
    VIRTUAL_FOR_TESTING
    int epoll_wait(int epfd, epoll_event* events,
            int maxEvents, int timeout) {
        return ::epoll_wait(epfd, events, maxEvents, timeout);
    }
    VIRTUAL_FOR_TESTING
    void exit(int status) {
        ::exit(status);
    }
    VIRTUAL_FOR_TESTING
    int ioctl(int fd, int reqType, void* request) {
        return ::ioctl(fd, reqType, request);
    }
    VIRTUAL_FOR_TESTING
    int fcntl(int fd, int cmd, int arg1) {
        return ::fcntl(fd, cmd, arg1);
    }
    VIRTUAL_FOR_TESTING
    int futexWait(int *addr, int value) {
        return static_cast<int>(::syscall(SYS_futex, addr, FUTEX_WAIT,
                value, NULL, NULL, 0));
    }
    VIRTUAL_FOR_TESTING
    int futexWake(int *addr, int count) {
        return static_cast<int>(::syscall(SYS_futex, addr, FUTEX_WAKE,
                count, NULL, NULL, 0));
    }
    VIRTUAL_FOR_TESTING
    size_t fwrite(const void *src, size_t size, size_t count, FILE* f) {
        return ::fwrite(src, size, count, f);
    }
    VIRTUAL_FOR_TESTING
    int getsockname(int sockfd, sockaddr* addr, socklen_t* addrlen) {
        return ::getsockname(sockfd, addr, addrlen);
    }
    VIRTUAL_FOR_TESTING
    int listen(int sockfd, int backlog) {
        return ::listen(sockfd, backlog);
    }
    VIRTUAL_FOR_TESTING
    int pipe(int fds[2]) {
        return ::pipe(fds);
    }
    VIRTUAL_FOR_TESTING
    ssize_t pread(int fd, void* buf, size_t count, off_t offset) {
        return ::pread(fd, buf, count, offset);
    }
    VIRTUAL_FOR_TESTING
    ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset) {
        return ::pwrite(fd, buf, count, offset);
    }
    VIRTUAL_FOR_TESTING
    ssize_t recv(int sockfd, void *buf, size_t len, int flags) {
        return ::recv(sockfd, buf, len, flags);
    }
    VIRTUAL_FOR_TESTING
    ssize_t recvfrom(int sockfd, void *buf, size_t len, int flags,
                     sockaddr *from, socklen_t* fromLen) {
        return ::recvfrom(sockfd, buf, len, flags, from, fromLen);
    }
    VIRTUAL_FOR_TESTING
    ssize_t recvmmsg(int sockfd, struct mmsghdr *msgvec, unsigned int vlen,
            unsigned int flags, struct timespec *timeout) {
        return ::recvmmsg(sockfd, msgvec, vlen, flags, timeout);
    }
    VIRTUAL_FOR_TESTING
    int select(int nfds, fd_set *readfds, fd_set *writefds,
           fd_set *errorfds, struct timeval *timeout)
    {
        return ::select(nfds, readfds, writefds, errorfds, timeout);
    }
    VIRTUAL_FOR_TESTING
    ssize_t sendmsg(int sockfd, const msghdr *msg, int flags) {
        return ::sendmsg(sockfd, msg, flags);
    }
    VIRTUAL_FOR_TESTING
    ssize_t sendto(int socket, const void *buffer, size_t length, int flags,
           const struct sockaddr *destAddr, socklen_t destLen)
    {
        return ::sendto(socket, buffer, length, flags, destAddr, destLen);
    }
    VIRTUAL_FOR_TESTING
    int setsockopt(int sockfd, int level, int optname, const void *optval,
                    socklen_t optlen) {
        return ::setsockopt(sockfd, level, optname, optval, optlen);
    }
    VIRTUAL_FOR_TESTING
    int socket(int domain, int type, int protocol) {
        return ::socket(domain, type, protocol);
    }
    VIRTUAL_FOR_TESTING
    ssize_t write(int fd, const void* buf, size_t count) {
        return ::write(fd, buf, count);
    }

    private:
    DISALLOW_COPY_AND_ASSIGN(Syscall);
};

/**
 * Used to set/restore static Syscall* class members for testing.
 */
struct SyscallGuard {
    SyscallGuard(Syscall** sys, Syscall* newSys)
        : sys(sys)
        , old(*sys)
    {
        *sys = newSys;
    }
    ~SyscallGuard() {
        *sys = old;
    }
    Syscall** sys;
    Syscall* old;
    DISALLOW_COPY_AND_ASSIGN(SyscallGuard);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_SYSCALL_H
