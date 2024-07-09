//////////////////////////////////////////////////////////////////////////
// File: Connection.h
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

#ifndef AMQP_CPP_CONNECTION_H__
#define AMQP_CPP_CONNECTION_H__

#include "amqp.h"
#include "noncopyable.h"
#include <map>
#include <memory>
#include <string>

namespace AMQP
{
#define AMQP_TIMEOUT_DEFAULT            45
#define AMQP_CONSUME_MAX_PREFETCH_COUNT 65535

struct ConnectionInfo
{
    std::string hostname;
    std::string vhost;
    std::string exchange;
    std::string routingkey;
    std::string username;
    std::string password;
    unsigned int port;
};

enum ChannelState
{
    READY = 0,
    USING = 1,
    CLOSED = 2
};

class Table;
class Channel;

/**
 * @brief TCP Connection
 */
class TCPConnection : noncopyable
{
    friend class Channel;

public:
    typedef std::shared_ptr<TCPConnection> ptr;
    typedef std::map<std::uint16_t, ChannelState> ChannelsList;

    /**
     * @brief Default contructor
     */
    TCPConnection();

    /**
     * @brief A contructor with host and port, this will also connect to the server
     */
    TCPConnection(const std::string& host, std::uint16_t port);
    ~TCPConnection() noexcept;

    /**
     * @brief Connect to amqp server with host and port, if was connected, do nothing.
     * Call explicitly disconnect(...) before change host.
     */
    virtual void connect(const std::string& host, std::uint16_t port);

    /**
     * @brief Login to broker
     */
    virtual void login(const std::string& vhost, const std::string& user, const std::string& pass,
                       const AMQP::Table* properties = nullptr, int heartBeat = 60,
                       int channelMax = 0, int frameMax = 131072,
                       amqp_sasl_method_enum saslMethod = AMQP_SASL_METHOD_PLAIN);
    template <typename... Args>
    inline void login(const std::string& vhost, const std::string& user, const std::string& pass,
                      int heartBeat, const AMQP::Table* properties, int channelMax, int frameMax,
                      amqp_sasl_method_enum saslMethod, Args&&...);

    /**
     * @brief Close connection, the function will be implicitly called by deconstructor.
     */
    virtual void disconnect() noexcept;

    /**
     * @brief Set rpc timeout for connection
     * @param timeOut
     */
    virtual void setRPCTimeOut(const struct timeval& timeOut);

    /**
     * @brief Create a channel. If specify channel number, but it is being used,
     * does nothings and return null, otherwise. If passing channel number <=0,
     * this function will generate the channel number for ready or closed.
     * @param channel: the number of channel
     */
    virtual std::unique_ptr<Channel> createChannel(std::int32_t id = 0);

    /**
     * @brief Get channel state
     * @param channel
     * @return
     */
    ChannelState getChannelState(std::uint16_t id);

    /**
     * @brief Looking for a usable channel, if not found, return 0
     */
    std::uint16_t getReadyChannel();

    bool isConnected() const { return _isConnected; };
    bool isLogined() const { return _isLogined; };
    std::uint16_t getPort() const { return _port; };
    std::string getHost() const { return _hostName; };
    std::string getVHost() const { return _hostName; };
    std::string getUser() const { return _username; };
    int getHeartBeat();

    /**
     * @brief Create and connect
     */
    static std::shared_ptr<TCPConnection> createConnection(const std::string& hostname,
                                                           std::uint16_t port);

    //template <typename... Args>
    //static std::shared_ptr<TCPConnection> createConnection(Args&&... args)
    //{
    //    return std::make_shared<TCPConnection>(std::forward<Args>(args)...);
    //}

    /**
     * @brief Assert rpc reply, if happend, throw an exception.
     */
    void assertRpcReply(const std::string& msgThrow);
    void assertRpcReply(const std::string& msgThrow, const amqp_rpc_reply_t& res);

protected:
    amqp_connection_state_t connnection() { return _pConn; };

    std::string _hostName;
    std::string _vHost;
    std::string _username;
    std::string _password;
    std::uint16_t _port{0};
    bool _isLogined{false};
    bool _isConnected{false};
    amqp_socket_t* _pSocket{nullptr};
    amqp_connection_state_t _pConn{nullptr};
    std::map<std::uint16_t, ChannelState> _channelsState;
};

//template <typename... Args>
//inline void AMQP::TCPConnection::login(const std::string& strVHost, const std::string& strUser,
//                                       const std::string& strPass, int heartBeat,
//                                       const AMQP::Table* properties, int channelMax, int frameMax,
//                                       amqp_sasl_method_enum saslMethod, Args&&... args)
//{
//    if (!isConnected()) return;
//
//    _vHost = strVHost;
//    _username = strUser;
//    _password = strPass;
//
//    amqp_rpc_reply_t res;
//
//    if (properties && properties->getEntriesSize() > 0)
//    {
//        amqp_table_t_ prps;
//        prps.entries = (amqp_table_entry_t_*)properties->getEntries();
//        prps.num_entries = (int)properties->getEntriesSize();
//
//        res = amqp_login_with_properties(_pConn, _vHost.c_str(), channelMax, frameMax, heartBeat,
//                                         &prps, saslMethod, _username.c_str(), _password.c_str(),
//                                         std::forward<Args>(args)...);
//    }
//    else
//    {
//        res = amqp_login(_pConn, _vHost.c_str(), channelMax, frameMax, heartBeat, saslMethod,
//                         _username.c_str(), _password.c_str(), std::forward<Args>(args)...);
//    }
//
//    assertRpcReply("Login failed", res);
//
//    _isLogined = true;
//}

// In plainning
class SSLConnection : public TCPConnection
{
public:
    SSLConnection(){};
    ~SSLConnection(){};

private:
};

} // namespace AMQP

#endif // !AMQP_CPP_CONNECTION_H__
