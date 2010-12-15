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

#ifndef RAMCLOUD_MOCKSYSCALL_H
#define RAMCLOUD_MOCKSYSCALL_H

#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>

#include "Common.h"
#include "Syscall.h"

namespace RAMCloud {
/**
 * This class is used as a replacement for Syscall during testing.  Each
 * method has the same name and interface as a Linux system call; by setting
 * variables that preceded the method definition it can be configured to
 * do various things such as call the normal system call or return an error.
 */
class MockSyscall : public Syscall {
    public:
    MockSyscall() : acceptErrno(0), bindErrno(0), closeErrno(0), closeCount(0),
                    connectErrno(0), fcntlErrno(0), listenErrno(0),
                    recvErrno(0), recvEof(false),
                    recvfromErrno(0), recvfromEof(false),
                    sendmsgErrno(0), sendmsgReturnCount(-1),
                    setsockoptErrno(0), socketErrno(0) {}

    int acceptErrno;
    int accept(int sockfd, struct sockaddr *addr, socklen_t *addrlen) {
        if (acceptErrno == 0) {
            return ::accept(sockfd, addr, addrlen);
        }
        errno = acceptErrno;
        return -1;
    }

    int bindErrno;
    int bind(int sockfd, const struct sockaddr *addr, socklen_t addrlen) {
        if (bindErrno == 0) {
            return ::bind(sockfd, addr, addrlen);
        }
        errno = bindErrno;
        return -1;
    }

    int closeErrno;
    int closeCount;
    int close(int fd) {
        closeCount++;
        if (closeErrno == 0) {
            return ::close(fd);
        }
        errno = closeErrno;
        return -1;
    }

    int connectErrno;
    int connect(int sockfd, const struct sockaddr *addr,
                socklen_t addrlen) {
        if (connectErrno == 0) {
            return ::connect(sockfd, addr, addrlen);
        }
        errno = connectErrno;
        return -1;
    }

    int fcntlErrno;
    int fcntl(int fd, int cmd, int arg1) {
        if (fcntlErrno == 0) {
            return ::fcntl(fd, cmd, arg1);
        }
        errno = fcntlErrno;
        return -1;
    }

    int listenErrno;
    int listen(int sockfd, int backlog) {
        if (listenErrno == 0) {
            return ::listen(sockfd, backlog);
        }
        errno = listenErrno;
        return -1;
    }

    int recvErrno;
    bool recvEof;
    ssize_t recv(int sockfd, void *buf, size_t len, int flags) {
        if (recvEof) {
            return 0;
        }
        if (recvErrno == 0) {
            return ::recv(sockfd, buf, len, flags);
        }
        errno = recvErrno;
        return -1;
    }

    int recvfromErrno;
    bool recvfromEof;
    ssize_t recvfrom(int sockfd, void *buf, size_t len, int flags,
                     sockaddr *from, socklen_t* fromLen) {
        if (recvfromEof) {
            return 0;
        }
        if (recvfromErrno == 0) {
            return ::recvfrom(sockfd, buf, len, flags, from, fromLen);
        }
        errno = recvfromErrno;
        return -1;
    }

    int sendmsgErrno;
    int sendmsgReturnCount;
    ssize_t sendmsg(int sockfd, const struct msghdr *msg, int flags) {
        if (sendmsgErrno != 0) {
            errno = sendmsgErrno;
            return -1;
        } else if (sendmsgReturnCount >= 0) {
            // Simulates a short-count write.
            return sendmsgReturnCount;
        }
        return ::sendmsg(sockfd, msg, flags);
    }

    int setsockoptErrno;
    int setsockopt(int sockfd, int level, int optname, const void *optval,
                    socklen_t optlen) {
        if (setsockoptErrno == 0) {
            return ::setsockopt(sockfd, level, optname, optval, optlen);
        }
        errno = setsockoptErrno;
        return -1;
    }

    int socketErrno;
    int socket(int domain, int type, int protocol) {
        if (socketErrno == 0) {
            return ::socket(domain, type, protocol);
        }
        errno = socketErrno;
        return -1;
    }

    private:
    DISALLOW_COPY_AND_ASSIGN(MockSyscall);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_MOCKSYSCALL_H
