//////////////////////////////////////////////////////////////////////////
// File: Channel.h
// Description: The implement of channel
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

#ifndef AMQP_CPP_CHANNEL_H__
#define AMQP_CPP_CHANNEL_H__

#include "Connection.h"
#include "amqp.h"
#include <string>
#include <utility>
#include <vector>

#define AMQP_FIELD_EXPIRES                "x-expires"
#define AMQP_FIELD_SINGLE_ACTIVE_CONSUMER "x-single-active-consumer"
#define AMQP_FIELD_CONNECTION_NAME        "connection_name"

namespace AMQP
{

class Table;

typedef amqp_bytes_t Message;

struct MessageProps
{
    std::string replyQueue;
    std::string correlationId;
    std::string contentType;
    std::string msgId;
    std::uint8_t deliveryMode;
    MessageProps()
    {
        replyQueue = "";
        correlationId = "";
        contentType = "";
        msgId = "";
        deliveryMode = 0;
    }
};

struct Envelope
{
    bool redelivered;
    std::uint16_t channelNum;
    std::uint64_t deliveryTag;
    std::string consumerTag;
    std::string exchange;
    std::string routingKey;
    AMQP::Message msg;

    Envelope()
    {
        redelivered = false;
        channelNum = 0;
        deliveryTag = 0;
        consumerTag = "";
        exchange = "";
        routingKey = "";
        msg.len = 0;
        msg.bytes = nullptr;
    }

    Envelope(bool redelivered, std::uint16_t channel, std::uint64_t delivery_tag,
             std::string consumer_tag, std::string exchange, std::string routing_key,
             AMQP::Message msg)
    {
        this->redelivered = redelivered;
        this->channelNum = channel;
        this->deliveryTag = delivery_tag;
        this->consumerTag = consumer_tag;
        this->exchange = exchange;
        this->routingKey = routing_key;
        this->msg = msg;
    }

    ~Envelope() { destroy(); }

    void destroy()
    {
        if (msg.bytes)
        {
            delete[] msg.bytes;
            msg.bytes = nullptr;
            msg.len = 0;
        }
    }

    Envelope(Envelope&& en) noexcept { *this = std::move(en); }
    Envelope& operator=(Envelope&& en) noexcept
    {
        if (this != &en)
        {
            redelivered = std::move(en.redelivered);
            channelNum = std::move(en.channelNum);
            deliveryTag = std::move(en.deliveryTag);
            consumerTag = std::move(en.consumerTag);
            exchange = std::move(en.exchange);
            routingKey = std::move(en.routingKey);
            msg.len = std::move(en.msg.len);
            msg.bytes = std::move(en.msg.bytes);

            en.msg.len = 0;
            en.msg.bytes = nullptr;
        }

        return *this;
    }

    Envelope(const Envelope& en) { *this = en; };
    Envelope& operator=(const Envelope& en)
    {
        if (this != &en)
        {
            redelivered = en.redelivered;
            channelNum = en.channelNum;
            deliveryTag = en.deliveryTag;
            consumerTag = en.consumerTag;
            exchange = en.exchange;
            routingKey = en.routingKey;
            msg.len = en.msg.len;

            msg.bytes = new char[msg.len];
            memcpy(msg.bytes, en.msg.bytes, msg.len);
        }

        return *this;
    }
};

class Table
{
public:
    typedef std::vector<amqp_table_entry_t_> Entries;

    Table();
    Table(const Table&);
    Table(Table&&) noexcept;
    ~Table() { empty(); };

    void addEntry(const char* key, const char* value);
    void addEntry(const char* key, bool value);
    void addEntry(const char* key, float value);
    void addEntry(const char* key, double value);
    void addEntry(const char* key, std::int8_t value);
    void addEntry(const char* key, std::uint8_t value);
    void addEntry(const char* key, std::int16_t value);
    void addEntry(const char* key, std::uint16_t value);
    void addEntry(const char* key, std::int32_t value);
    void addEntry(const char* key, std::uint32_t value);
    template <typename T, typename... Args>
    void addEntry(const char* key, const T& value, const char* key2, Args&&... args)
    {
        addEntry(key, value);
        addEntry(key2, std::forward<Args>(args)...);
    }
    void removeEntry(const char* key);

    const amqp_table_entry_t_* getEntries() const
    {
        if (_entries) return _entries->data();
        return nullptr;
    }
    std::size_t getEntriesSize() const
    {
        if (_entries) return _entries->size();
        return 0;
    }

    Table& operator=(const Table&);
    Table& operator=(Table&&) noexcept;

    void empty();

private:
    std::shared_ptr<std::vector<amqp_table_entry_t_>> _entries;
};

class Channel : noncopyable
{
    friend class TCPConnection;

public:
    typedef std::shared_ptr<Channel> ptr;
    typedef std::unique_ptr<Channel> u_ptr;

    virtual ~Channel();

    AMQP::TCPConnection& connection() { return _tcpConn; };

    /**
     * Consume on queue.
     *
     * @param strQueue
     * @param consumerTag
     * @param bNoLocal
     * @param bNoAck
     * @param bExclusive
     * @param args
     * @return consume tag
     */
    virtual std::string basicConsume(const std::string& strQueue,
                                     const std::string& consumerTag = "", bool bNoLocal = false,
                                     bool bNoAck = false, bool bExclusive = false,
                                     const Table* args = nullptr);
    virtual std::string basicConsume(amqp_bytes_t strQueue, const std::string& consumerTag = "",
                                     bool bNoLocal = false, bool bNoAck = false,
                                     bool bExclusive = false, const Table* args = nullptr);

    virtual void basicCancel(const std::string& consumerTag);

    virtual AMQP::Envelope getMessage(const struct timeval& timeOut) final;

    /**
     * @brief Ack a message.
     * @param en: Evenlope to ack
     * @param multiple
     */
    virtual void basicAck(const AMQP::Envelope& en, bool multiple = false);
    virtual void basicAck(std::uint64_t deliveryTag, bool multiple = false);

    virtual void basicQos(std::uint16_t perfectCount, std::uint32_t perfectSize = 0,
                          bool global = false);

    /**
     * @brief Send a message to broker, automatically connect if necessary.
     */
    virtual void basicPublish(const std::string& exchange, const std::string& routingKey,
                              const AMQP::Message& message,
                              const AMQP::MessageProps* msgProps = nullptr);
    virtual void basicPublish(const std::string& exchange, const std::string& routingKey,
                              const std::string& message,
                              const AMQP::MessageProps* msgProps = nullptr);

    /**
     * Declare an exchange.
     * 
     * @param exchangeName
     * @param type
     * @param passive
     * @param durable
     * @param autoDel
     * @param internal
     * @param noWait
     * @param args
     * @return The exchange name
     */
    virtual std::string declareExchange(std::string exchangeName, std::string type = "direct",
                                        bool passive = false, bool durable = true,
                                        bool autoDel = false, bool internal = false,
                                        bool noWait = true, const AMQP::Table* args = nullptr);

    virtual void bindExchange(std::string destination, std::string source, std::string routingKey,
                              const AMQP::Table* args = nullptr);

    virtual void deleteExchange(std::string exchange, bool ifUnUsed = true, bool noWait = false);

    virtual void unbindExchange(std::string destination, std::string source, std::string routingKey,
                                const AMQP::Table* args = nullptr);

    /**
     * Declare a queue.
     * 
     * @param queueName
     * @param passive
     * @param durable
     * @param exclusive
     * @param autoDelete
     * @param args
     * @return The queue name
     */
    virtual std::string declareQueue(const std::string& queueName, bool passive = false,
                                     bool durable = false, bool exclusive = false,
                                     bool autoDelete = false, const AMQP::Table* args = nullptr);

    virtual void bindQueue(const std::string& exchange, const std::string& queue,
                           const std::string& bindingkey);
    virtual void unbindQueue(const std::string& exchange, const std::string& queue,
                             const std::string& bindingkey);
    virtual void purgeQueue(const std::string& queue);
    virtual void deleteQueue(const std::string& queue, bool ifUnused = true, bool ifEmpty = true);

    /**
     * @brief Close channel. This will be implicitly called by deconstructor.
     */
    void close();

    bool isOpened() { return _tcpConn.getChannelState(_channelId) == USING; };
    std::uint16_t getId() { return _channelId; }

protected:
    Channel(AMQP::TCPConnection&, std::uint16_t id = 1);

    virtual void open(std::uint16_t id = 1);
    virtual void assertConnection();
    virtual void dumpBasicProps(const AMQP::MessageProps* pProps,
                                amqp_basic_properties_t& pAmqpProps);
    virtual void initBasicProps(amqp_basic_properties_t& pAmqpProps);
    virtual void updateChannelState(ChannelState state);

    AMQP::TCPConnection& const _tcpConn;
    const std::uint16_t _channelId;
};
} // namespace AMQP

#endif // !AMQP_CPP_CHANNEL_H__
