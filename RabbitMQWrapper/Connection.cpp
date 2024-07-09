//////////////////////////////////////////////////////////////////////////
// File: Connection.cpp
// Description: The implement of amqp connection
// Author: Le Xuan Tuan Anh
//
// Copyright 2022 Le Xuan Tuan Anh
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//////////////////////////////////////////////////////////////////////////
#include "pch.h"
#include "Connection.h"
#include "Channel.h"
#include "Exception.h"
#include <amqp_tcp_socket.h>
#include <limits>

using namespace AMQP;

TCPConnection::TCPConnection() {}

AMQP::TCPConnection::TCPConnection(const std::string& host, uint16_t port) : TCPConnection()
{
    connect(host, port);
}

AMQP::TCPConnection::~TCPConnection()
{ /*if (isConnected()) */
    disconnect();
}

void AMQP::TCPConnection::connect(const std::string& host, uint16_t port)
{
    if (isConnected()) return;

    _hostName = host;
    _port = port;

    // create and open socket
    {
        _pConn = amqp_new_connection();
        _pSocket = amqp_tcp_socket_new(_pConn);
        if (!_pSocket)
        {
            throw Exception("Connect to host failed", AMQP_STATUS_SOCKET_ERROR);
        }
        int status = amqp_socket_open(_pSocket, _hostName.c_str(), _port);
        if (status)
        {
            throw Exception("Connect to host failed", AMQP_STATUS_SOCKET_ERROR);
        }
    }
    _isConnected = true;
}

void AMQP::TCPConnection::login(const std::string& strVHost, const std::string& strUser,
                                const std::string& strPass, const Table* properties /*= nullptr*/,
                                int heartBeat /*= 60*/, int channelMax /*= 0*/,
                                int frameMax /*= 131072*/,
                                amqp_sasl_method_enum saslMethod /*= AMQP_SASL_METHOD_PLAIN*/)
{
    //login(strVHost, strUser, strPass, heartBeat, properties, channelMax, frameMax, saslMethod);
    if (!isConnected()) return;

    _vHost = strVHost;
    _username = strUser;
    _password = strPass;

    amqp_rpc_reply_t res;

    if (properties && properties->getEntriesSize() > 0)
    {
        amqp_table_t_ prps;
        prps.entries = (amqp_table_entry_t_*)properties->getEntries();
        prps.num_entries = (int)properties->getEntriesSize();

        res = amqp_login_with_properties(_pConn, _vHost.c_str(), channelMax, frameMax, heartBeat,
            &prps, saslMethod, _username.c_str(), _password.c_str());
    }
    else
    {
        res = amqp_login(_pConn, _vHost.c_str(), channelMax, frameMax, heartBeat, saslMethod,
            _username.c_str(), _password.c_str());
    }

    assertRpcReply("Login failed", res);

    _isLogined = true;
}

void AMQP::TCPConnection::disconnect() noexcept
{
    amqp_connection_close(_pConn, AMQP_REPLY_SUCCESS);
    int r = amqp_destroy_connection(_pConn);
    _isConnected = false;
}

void AMQP::TCPConnection::setRPCTimeOut(const timeval& timeOut)
{
    if (isConnected()) amqp_set_rpc_timeout(_pConn, &timeOut);
}

int AMQP::TCPConnection::getHeartBeat() { return amqp_get_heartbeat(_pConn); }

std::shared_ptr<TCPConnection> AMQP::TCPConnection::createConnection(const std::string& host,
                                                                     std::uint16_t port)
{
    return std::make_shared<TCPConnection>(host, port);
}

AMQP::Channel::u_ptr AMQP::TCPConnection::createChannel(int32_t id)
{
    if (!isLogined()) throw AMQP::Exception("The connection is not existed.");
    // if specify channel number, but it is being used, does nothings.
    if (id > 0 && (getChannelState(id) == ChannelState::USING))
        throw AMQP::Exception("The channel is using.");
    // if get default arguments, get next ready channel
    if (id <= 0) id = getReadyChannel();
    if (id > 0) return std::unique_ptr<Channel>(new Channel(*this, (std::uint16_t)id));
    return nullptr;
}

ChannelState AMQP::TCPConnection::getChannelState(std::uint16_t id)
{
    auto resFound = _channelsState.find(id);
    if (resFound == _channelsState.end()) return ChannelState::READY;
    return resFound->second;
}

std::uint16_t AMQP::TCPConnection::getReadyChannel()
{
    constexpr auto maxNum = (std::numeric_limits<std::uint16_t>::max)();
    std::uint16_t channel = 1;
    ChannelsList::const_iterator found = _channelsState.find(channel);

    while (found != _channelsState.end())
    {
        if (found->second == ChannelState::READY || found->second == ChannelState::CLOSED) break;
        if (channel == maxNum) return 0;
        found = _channelsState.find(++channel);
    }

    return channel;
}

void AMQP::TCPConnection::assertRpcReply(const std::string& msgThrow)
{
    auto res = amqp_get_rpc_reply(_pConn);
    if (res.reply_type != AMQP_RESPONSE_NORMAL) throw Exception(msgThrow, res);
}

void AMQP::TCPConnection::assertRpcReply(const std::string& msgThrow, const amqp_rpc_reply_t& res)
{
    if (res.reply_type != AMQP_RESPONSE_NORMAL) throw Exception(msgThrow, res);
}
