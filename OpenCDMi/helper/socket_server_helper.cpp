/*
 * Copyright (c) 2018, Linaro Limited
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include <string.h>
#include <unistd.h>
#include <rpc/rpc.h>
#include <sys/signal.h>
#include <sys/time.h>

#include <fstream>
#include <iostream>
#include <map>
#include <thread>
#include <string>
#include <vector>

#include "cdmi.h"
#include "cdmi-log.h"



#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>

#include <fstream>
#include "cdmi.h"
#include "cdmi-log.h"

#include "socket/socket_server_helper.h"

USE_NAMESPACE_OCDM();
using namespace std;

int SocketServer::sm_NextUniqueId = 0;

int SocketServer::Connect(int f_SocketChannelId)
{
  int status = 0;
  int lSocketFd = SOCKET_INVALID_FD; // Listening socket
  struct sockaddr_un socketAddress;
  uint32_t trials = 10000;
  struct timeval timeout_tv;

  if(m_SocketFd >= 0) {
    CDMI_DLOG() << "Socket connection already established, closing connection";
    Disconnect();
  }

  if(f_SocketChannelId < 0) {
    CDMI_ELOG() << "Invalid socket channel ID";
    status = -1;
    goto handle_error;
  }

  lSocketFd = socket(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if(lSocketFd < 0) {
    CDMI_ELOG() << "Failure to create socket";
    status = -1;
    goto handle_error;
  }

  /* Use abstract socket (First byte is \0). */
  memset(&socketAddress, 0, sizeof(socketAddress));
  socketAddress.sun_family = AF_UNIX;
  sprintf(&socketAddress.sun_path[1], "opencdm_fd_communication_channel_0x%08x", f_SocketChannelId);

  CDMI_DLOG() << "Binding socket (" << &socketAddress.sun_path[1] << ")";

  if(bind(lSocketFd,  (struct sockaddr *)&socketAddress, sizeof(socketAddress)) < 0) {
    CDMI_ELOG() << "Failure to bind socket";
    status = -1;
    goto handle_error;
  }

  if(listen(lSocketFd, 1) < 0) {
    CDMI_ELOG() << "Failure to set to listen state";
    status = -1;
    goto handle_error;
  }

  while(trials > 0)
  {
    m_SocketFd = accept(lSocketFd, NULL, NULL);
    if(m_SocketFd >= 0) {
      /* Connection accepted */
      break;
    } else if(errno != EWOULDBLOCK &&
              errno != EAGAIN) {
      CDMI_ELOG() << "Failure to accept connection";
      status = -1;
      goto handle_error;
    }

    usleep(1000);
    trials--;
  }
  if(trials == 0) {
    CDMI_ELOG() << "Timeout to accept connection";
    status = -1;
    goto handle_error;
  }

  /* Configure a timeout for recvmsg(). This is to avoid an infinite wait
   in case of an error. */
  timeout_tv.tv_sec = SOCKET_RECEIVE_TIMEOUT;
  timeout_tv.tv_usec = 0;
  status = setsockopt(m_SocketFd, SOL_SOCKET, SO_RCVTIMEO, (const void *)&timeout_tv, sizeof(timeout_tv));
  if(status < 0) {
    CDMI_ELOG() << "Cannot configure recvmsg timeout";
    goto handle_error;
  }

  CDMI_DLOG() << "SocketServer::Connect(): Connection is established";

handle_error:
  if(status < 0) {
    Disconnect();
  }

  if(lSocketFd >= 0) {
    // Listening socket may be closed
    close(lSocketFd);
    lSocketFd = SOCKET_INVALID_FD;
  }

  return status;
}

int SocketServer::ReceiveFileDescriptor(int &f_FileDescriptor, uint32_t &f_Size)
{
  int status = 0;
  struct msghdr msg;
  struct iovec iov;
  ssize_t actualSize = 0;

  /* Control message buffer contains the control message structure plus
     one file descriptor. */
  #define CMSG_SIZE (sizeof(struct cmsghdr) + sizeof(int))
  uint8_t cmsg_buffer[CMSG_SIZE] = {0};
  struct cmsghdr *cmsg; // Pointer to control message

  if(m_SocketFd < 0) {
    CDMI_ELOG() << "Invalid socket file descriptor";
    status = -1;
    goto handle_error;
  }

  /* Get buffer size with the file descriptor */
  iov.iov_base = &f_Size;
  iov.iov_len  = sizeof(f_Size);

  msg.msg_name       = NULL;
  msg.msg_namelen    = 0;
  msg.msg_iov        = &iov;
  msg.msg_iovlen     = 1;
  msg.msg_control    = cmsg_buffer;
  msg.msg_controllen = CMSG_SIZE;
  msg.msg_flags      = 0; /* ignored */

  cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_len = CMSG_SIZE;
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  *(int *)CMSG_DATA(cmsg) = -1;

  // This call will timeout after SOCKET_RECEIVE_TIMEOUT seconds
  actualSize = recvmsg(m_SocketFd, &msg, 0);
  if(actualSize < 0) {
    if(errno == EWOULDBLOCK || errno == EAGAIN)
      CDMI_ELOG() << "Cannot receive FD (Timeout)";
    else
      CDMI_ELOG() << "Cannot receive FD";
    status = -1;
    goto handle_error;
  } else if((size_t)actualSize < sizeof(f_Size)) {
    CDMI_ELOG() << "Data received is too small";
    status = -1;
    goto handle_error;
  }

  f_FileDescriptor = *(int *)CMSG_DATA(cmsg);
  /*if(f_FileDescriptor < 0) {
    CDMI_ELOG() << "Invalid FD received";
    status = -1;
  }*/

  CDMI_DLOG() << "SocketServer::ReceiveFileDescriptor(): File descriptor is " << f_FileDescriptor;
  CDMI_DLOG() << "SocketServer::ReceiveFileDescriptor(): File descriptor references " << f_Size << " bytes";

handle_error:
  if(status < 0) {
    f_FileDescriptor = -1;
    f_Size = 0;
  }
  return status;
}
