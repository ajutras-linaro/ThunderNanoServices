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

#ifndef __SOCKET_SERVER_HELPER_H__
#define __SOCKET_SERVER_HELPER_H__

#include <unistd.h>

#define SOCKET_INVALID_FD (-1)
#define SOCKET_RECEIVE_TIMEOUT (1) // second(s)

class SocketServer
{
public:
  // Constructor.
  SocketServer(void) {
    m_SocketFd = SOCKET_INVALID_FD;
  }

  // Destructor.
  ~SocketServer(void) {
    Disconnect();
  }

  int Connect(int f_SocketChannelId);

  void Disconnect(void) {
    if(m_SocketFd >= 0) {
      close(m_SocketFd);
      m_SocketFd = SOCKET_INVALID_FD;
    }
  }

  int ReceiveFileDescriptor(int &f_FileDescriptor, uint32_t &f_Size);

  void CloseFileDescriptor(int f_FileDescriptor) {
    if(f_FileDescriptor >= 0)
      close(f_FileDescriptor);
  }

  static int GetUniqueId(void) {
    if(sm_NextUniqueId < INT_MAX)
      return sm_NextUniqueId++;
    else
      return (sm_NextUniqueId = 0); // Wrap around to avoid negative integers
  }

private:
  int m_SocketFd;  // Connected socket

  int m_SocketChannelId;

  static int sm_NextUniqueId;
};

#endif  // #ifdef __SOCKET_SERVER_HELPER_H__
