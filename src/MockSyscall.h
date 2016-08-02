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

#ifndef RAMCLOUD_MOCKSYSCALL_H
#define RAMCLOUD_MOCKSYSCALL_H

#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <net/if_arp.h>

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
                    connectErrno(0), epollCreateErrno(0), epollCtlErrno(0),
                    epollWaitCount(-1), epollWaitEvents(NULL),
                    epollWaitErrno(0), exitCount(0), fcntlErrno(0),
                    futexWaitErrno(0), futexWakeErrno(0), fwriteResult(~0LU),
                    getsocknameErrno(0), ioctlErrno(0),
                    ioctlRetriesToSuccess(0), listenErrno(0), pipeErrno(0),
                    recvErrno(0), recvEof(false), recvfromErrno(0),
                    recvfromEof(false), recvmmsgErrno(0),
                    sendmsgErrno(0), sendmsgReturnCount(-1),
                    sendtoErrno(0), sendtoReturnCount(-1), setsockoptErrno(0),
                    socketErrno(0), writeErrno(0) {}

    int acceptErrno;
    int accept(int sockfd, sockaddr *addr, socklen_t *addrlen) {
        if (acceptErrno == 0) {
            return ::accept(sockfd, addr, addrlen);
        }
        errno = acceptErrno;
        return -1;
    }

    int bindErrno;
    int bind(int sockfd, const sockaddr *addr, socklen_t addrlen) {
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
    int connect(int sockfd, const sockaddr *addr,
                socklen_t addrlen) {
        if (connectErrno == 0) {
            return ::connect(sockfd, addr, addrlen);
        }
        errno = connectErrno;
        return -1;
    }

    int epollCreateErrno;
    int epoll_create(int size) {
        if (epollCreateErrno == 0) {
            return ::epoll_create(size);
        }
        errno = epollCreateErrno;
        return -1;
    }

    int epollCtlErrno;
    int epoll_ctl(int epfd, int op, int fd, epoll_event *event) {
        if (epollCtlErrno == 0) {
            return ::epoll_ctl( epfd, op, fd, event);
        }
        errno = epollCtlErrno;
        return -1;
    }

    int epollWaitCount;
    epoll_event* epollWaitEvents;
    int epollWaitErrno;
    int epoll_wait(int epfd, epoll_event* events,
            int maxEvents, int timeout) {
        if (epollWaitCount >= 0) {
            memcpy(events, epollWaitEvents,
                   epollWaitCount*sizeof(epoll_event));
            int result = epollWaitCount;
            epollWaitCount = -1;
            return result;
        }
        if (epollWaitErrno == 0) {
            return ::epoll_wait(epfd, events, maxEvents, timeout);
        }
        errno = epollWaitErrno;
        return -1;
    }

    int exitCount;
    void exit(int status) {
        exitCount++;
        return;
    }

    int fcntlErrno;
    int fcntl(int fd, int cmd, int arg1) {
        if (fcntlErrno == 0) {
            return ::fcntl(fd, cmd, arg1);
        }
        errno = fcntlErrno;
        return -1;
    }

    int futexWaitErrno;
    int futexWait(int *addr, int value) {
        if (futexWaitErrno == 0) {
            return static_cast<int>(::syscall(SYS_futex, addr, FUTEX_WAIT,
                    value, NULL, NULL, 0));
        }
        errno = futexWaitErrno;
        futexWaitErrno = 0;
        return -1;
    }

    int futexWakeErrno;
    int futexWake(int *addr, int count) {
        if (futexWakeErrno == 0) {
            return static_cast<int>(::syscall(SYS_futex, addr, FUTEX_WAKE,
                    count, NULL, NULL, 0));
        }
        errno = futexWakeErrno;
        futexWakeErrno = 0;
        return -1;
    }

    size_t fwriteResult;
    size_t fwrite(const void *src, size_t size, size_t count, FILE* f) {
        if (fwriteResult == ~0LU) {
            return ::fwrite(src, size, count, f);
        }
        size_t result = fwriteResult;
        fwriteResult = ~0LU;
        return result;
    }

    int getsocknameErrno;
    int getsockname(int sockfd, sockaddr* addr, socklen_t* addrlen) {
        if (getsocknameErrno == 0) {
            return ::getsockname(sockfd, addr, addrlen);
        }
        errno = getsocknameErrno;
        getsocknameErrno = 0;
        return -1;
    }

    int ioctlErrno;
    int ioctlRetriesToSuccess;
    int ioctl(int fd, int reqType, void* request) {
        if (ioctlErrno != 0) {
            errno = ioctlErrno;
            return -1;
        } else if (reqType == SIOCGARP && ioctlRetriesToSuccess > 0) {

            // Simulates when kernel ARP cache is busy and not accessible.
            ioctlRetriesToSuccess--;
            struct arpreq* arpReq = reinterpret_cast<struct arpreq*>(request);
            arpReq->arp_flags = 0;
            return 0;
        } else {
            return ::ioctl(fd, reqType, request);
        }
    }

    int listenErrno;
    int listen(int sockfd, int backlog) {
        if (listenErrno == 0) {
            return ::listen(sockfd, backlog);
        }
        errno = listenErrno;
        return -1;
    }

    int pipeErrno;
    int pipe(int fds[2]) {
        if (pipeErrno == 0) {
            return ::pipe(fds);
        }
        errno = pipeErrno;
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

    int recvmmsgErrno;
    ssize_t recvmmsg(int sockfd, struct mmsghdr *msgvec, unsigned int vlen,
                     unsigned int flags, struct timespec *timeout) {
        if (recvmmsgErrno == 0) {
            return ::recvmmsg(sockfd, msgvec, vlen, flags, timeout);
        }
        errno = recvmmsgErrno;
        recvmmsgErrno = 0;
        return -1;

    }

    int sendmsgErrno;
    int sendmsgReturnCount;
    ssize_t sendmsg(int sockfd, const msghdr *msg, int flags) {
        if (sendmsgErrno != 0) {
            errno = sendmsgErrno;
            return -1;
        } else if (sendmsgReturnCount >= 0) {
            // Simulates a short-count write.
            return sendmsgReturnCount;
        }
        return ::sendmsg(sockfd, msg, flags);
    }

    int sendtoErrno;
    int sendtoReturnCount;
    ssize_t sendto(int socket, const void *buffer, size_t length, int flags,
           const struct sockaddr *destAddr, socklen_t destLen) {
        if (sendtoErrno != 0) {
            errno = sendtoErrno;
            return -1;
        } else if (sendtoReturnCount >= 0) {
            return sendtoReturnCount;
        }
        return ::sendto(socket, buffer, length, flags, destAddr, destLen);
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

    int writeErrno;
    ssize_t write(int fd, const void* buf, size_t count) {
        if (writeErrno == 0) {
            return ::write(fd, buf, count);
        }
        errno = writeErrno;
        return -1;
    }

    private:
    DISALLOW_COPY_AND_ASSIGN(MockSyscall);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_MOCKSYSCALL_H
