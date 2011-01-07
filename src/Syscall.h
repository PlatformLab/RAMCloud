/* Copyright (c) 2010 Stanford University
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

#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>

#include "Common.h"

namespace RAMCloud {
/**
 * This class provides a mechanism for invoking system calls that makes
 * it easy to intercept the calls with a mock class (e.g. MockSyscall)
 * for testing. When system calls are invoked through this base class
 * they have the same behavior as if they were invoked directly.
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
    int fcntl(int fd, int cmd, int arg1) {
        return ::fcntl(fd, cmd, arg1);
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
    ssize_t recv(int sockfd, void *buf, size_t len, int flags) {
        return ::recv(sockfd, buf, len, flags);
    }
    VIRTUAL_FOR_TESTING
    ssize_t recvfrom(int sockfd, void *buf, size_t len, int flags,
                     sockaddr *from, socklen_t* fromLen) {
        return ::recvfrom(sockfd, buf, len, flags, from, fromLen);
    }
    VIRTUAL_FOR_TESTING
    ssize_t sendmsg(int sockfd, const msghdr *msg, int flags) {
        return ::sendmsg(sockfd, msg, flags);
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

}  // namespace RAMCloud

#endif  // RAMCLOUD_SYSCALL_H
