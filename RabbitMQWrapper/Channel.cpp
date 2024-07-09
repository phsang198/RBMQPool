//////////////////////////////////////////////////////////////////////////
// File: Channel.cpp
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
#include "pch.h"
#include "Channel.h"
#include "Exception.h"

using namespace AMQP;

std::string AMQP::Channel::basicConsume(const std::string& strQueue, const std::string& consumerTag,
                                        bool bNoLocal /*= false*/, bool bNoAck /*= false*/,
                                        bool bExclusive /*= false*/, const Table* args)
{
    return basicConsume(amqp_cstring_bytes(strQueue.c_str()), consumerTag, bNoLocal, bNoAck,
                        bExclusive, args);
}

void AMQP::Channel::basicCancel(const std::string& consumerTag)
{
    auto r = amqp_basic_cancel(_tcpConn.connnection(), this->_channelId,
                               amqp_cstring_bytes(consumerTag.c_str()));
    if (!r)
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Cancel on queue failed", res);
    }
}

AMQP::Envelope AMQP::Channel::getMessage(const timeval& timeOut)
{
    Envelope en;
    amqp_connection_state_t conn = _tcpConn.connnection();
    char* pBuffer = nullptr;
    amqp_frame_t frame;
    int result = 0;
    size_t body_target = 0;
    size_t body_received = 0;

    for (;;)
    {
        amqp_maybe_release_buffers(conn);

        result = amqp_simple_wait_frame_noblock(conn, &frame, (timeval*)&timeOut);
        if (result < 0) throw AMQP::Exception("Wait frame failed", result);

        if (frame.frame_type != AMQP_FRAME_METHOD) continue;

        if (frame.payload.method.id != AMQP_BASIC_DELIVER_METHOD) continue;

        amqp_basic_deliver_t* deliver_method =
            reinterpret_cast<amqp_basic_deliver_t*>(frame.payload.method.decoded);

        en.exchange =
            std::string((char*)deliver_method->exchange.bytes, deliver_method->exchange.len);
        en.exchange =
            std::string((char*)deliver_method->routing_key.bytes, deliver_method->routing_key.len);
        en.exchange = std::string((char*)deliver_method->consumer_tag.bytes,
                                  deliver_method->consumer_tag.len);
        en.deliveryTag = deliver_method->delivery_tag;
        en.redelivered = (deliver_method->redelivered == 0 ? false : true);
        en.channelNum = frame.channel;

        result = amqp_simple_wait_frame_noblock(conn, &frame, (timeval*)&timeOut);
        if (result < 0) throw AMQP::Exception("Wait frame failed", result);

        if (frame.frame_type != AMQP_FRAME_HEADER)
        {
            throw AMQP::Exception("Received frame type is not header frame");
        }

        body_target = (size_t)frame.payload.properties.body_size;
        body_received = 0;

        if (body_target >= 0)
        {
            pBuffer = new char[body_target];

            while (body_received < body_target)
            {
                result = amqp_simple_wait_frame_noblock(conn, &frame, (timeval*)&timeOut);
                if (result < 0) throw AMQP::Exception("Wait frame", result);

                if (frame.frame_type != AMQP_FRAME_BODY)
                {
                    throw AMQP::Exception("Received frame type is not body frame");
                }

                memcpy(pBuffer + body_received, frame.payload.body_fragment.bytes,
                       frame.payload.body_fragment.len);
                body_received += frame.payload.body_fragment.len;
            }

            if (body_received != body_target)
            {
                if (pBuffer)
                {
                    delete[] pBuffer;
                    pBuffer = nullptr;
                }
                throw AMQP::Exception("Get message error: Body received less than target");
            }
        }

        break;
    }
    if (body_received > 0)
    {
        en.msg.bytes = pBuffer;
        en.msg.len = body_received;
    }

    pBuffer = nullptr;

    return en;
}

void AMQP::Channel::basicAck(const AMQP::Envelope& en, bool multiple /*= false*/)
{
    return basicAck(en.deliveryTag, multiple);
}

void AMQP::Channel::basicQos(std::uint16_t prefetchCount, std::uint32_t prefetchSize, bool global)
{
    amqp_connection_state_t conn = _tcpConn.connnection();
    /* If there is a limit, set the qos to match */
    if (prefetchCount > 0 && prefetchCount <= AMQP_CONSUME_MAX_PREFETCH_COUNT)
        if (!amqp_basic_qos(conn, _channelId, prefetchSize, prefetchCount, global))
        {
            auto res = amqp_get_rpc_reply(_tcpConn.connnection());
            throw AMQP::Exception("Basic Qos failed", res);
        }
}

void AMQP::Channel::basicAck(uint64_t m_delivery_tag, bool multiple /*= false*/)
{
    int res = amqp_basic_ack(_tcpConn.connnection(), this->_channelId, m_delivery_tag, multiple);
    if (0 > res)
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Ack message failed", res);
    }
}

void AMQP::Channel::basicPublish(const std::string& strExchange, const std::string& strRoutingKey,
                                 const AMQP::Message& message,
                                 const MessageProps* pProps /*= nullptr*/)
{
    amqp_connection_state_t conn = _tcpConn.connnection();
    amqp_basic_properties_t amqpProps;
    initBasicProps(amqpProps);

    if (pProps) dumpBasicProps(pProps, amqpProps);

    int res = amqp_basic_publish(conn, _channelId, amqp_cstring_bytes(strExchange.c_str()),
                                 amqp_cstring_bytes(strRoutingKey.c_str()), 0, 0, &amqpProps,
                                 (amqp_bytes_t)message);

    if (0 > res)
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Publish message failed", res);
    }

    if (pProps)
        if (!pProps->replyQueue.empty()) amqp_bytes_free(amqpProps.reply_to);
}

void AMQP::Channel::basicPublish(const std::string& strExchange, const std::string& strRoutingKey,
                                 const std::string& strMessage,
                                 const MessageProps* pProps /*= nullptr*/)
{
    basicPublish(strExchange, strRoutingKey, amqp_cstring_bytes(strMessage.c_str()), pProps);
}

std::string AMQP::Channel::declareExchange(std::string exchangeName, std::string type, bool passive,
                                           bool durable, bool autoDel, bool internal, bool noWait,
                                           const AMQP::Table* args)
{
    amqp_bytes_t exchange;

    if (exchangeName.empty())
        exchange = amqp_empty_bytes;
    else
        exchange = amqp_cstring_bytes(exchangeName.c_str());

    amqp_table_t table = amqp_empty_table;
    if (args && args->getEntriesSize() > 0)
    {
        table.num_entries = args->getEntriesSize();
        table.entries = (amqp_table_entry_t_*)args->getEntries();
    }

    amqp_exchange_declare_ok_t* r = amqp_exchange_declare(
        _tcpConn.connnection(), _channelId, exchange, amqp_cstring_bytes(type.c_str()), passive,
        durable, autoDel, internal, table);

    if (!r)
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Declare exchange failed", res);
    }

    return exchangeName;
}

void AMQP::Channel::bindExchange(std::string destination, std::string source, std::string routingKey,
                                const AMQP::Table* args)
{
    amqp_table_t table = amqp_empty_table;
    if (args && args->getEntriesSize() > 0)
    {
        table.num_entries = args->getEntriesSize();
        table.entries = (amqp_table_entry_t_*)args->getEntries();
    }

    amqp_exchange_bind_ok_t* r = amqp_exchange_bind(
        _tcpConn.connnection(), _channelId, amqp_cstring_bytes(destination.c_str()),
        amqp_cstring_bytes(source.c_str()), amqp_cstring_bytes(routingKey.c_str()), table);

    if (!r)
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Bind on exchange failed", res);
    }
}

void AMQP::Channel::deleteExchange(std::string exchange, bool ifUnUsed, bool noWait)
{
    amqp_exchange_delete_t req;
    req.ticket = 0;
    req.exchange = amqp_cstring_bytes(exchange.c_str());
    req.if_unused = ifUnUsed;
    req.nowait = noWait;

    if (!amqp_simple_rpc_decoded(_tcpConn.connnection(), _channelId, AMQP_EXCHANGE_DELETE_METHOD,
                                 AMQP_EXCHANGE_DELETE_OK_METHOD, &req))
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Delete the exchange failed", res);
    }
}

void AMQP::Channel::unbindExchange(std::string destination, std::string source,
                                  std::string routingKey, const AMQP::Table* args)
{
    amqp_table_t table = amqp_empty_table;
    if (args && args->getEntriesSize() > 0)
    {
        table.num_entries = args->getEntriesSize();
        table.entries = (amqp_table_entry_t_*)args->getEntries();
    }

    amqp_exchange_unbind_t req;
    req.ticket = 0;
    req.destination = amqp_cstring_bytes(destination.c_str());
    req.source = amqp_cstring_bytes(source.c_str());
    req.routing_key = amqp_cstring_bytes(routingKey.c_str());
    req.nowait = 0;
    req.arguments = table;

    if (!amqp_simple_rpc_decoded(_tcpConn.connnection(), _channelId, AMQP_EXCHANGE_UNBIND_METHOD,
                                 AMQP_EXCHANGE_UNBIND_OK_METHOD, &req))
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Unbind on the exchange failed", res);
    }
}

void AMQP::Channel::deleteQueue(const std::string& queue, bool ifUnused, bool ifEmpty)
{
    if (!amqp_queue_delete(_tcpConn.connnection(), _channelId, amqp_cstring_bytes(queue.c_str()),
                           ifUnused, ifEmpty))
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Delete the queue failed", res);
    }
}

void AMQP::Channel::purgeQueue(const std::string& queue)
{
    if (!amqp_queue_purge(_tcpConn.connnection(), _channelId, amqp_cstring_bytes(queue.c_str())))
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Purge on the queue failed", res);
    }
}

std::string AMQP::Channel::declareQueue(const std::string& queue, bool passive /*= false*/,
                                        bool durable /*= false*/, bool exclusive /*= false*/,
                                        bool autoDel /*= false*/,
                                        const AMQP::Table* args /*= AMQP::Table()*/)
{
    amqp_bytes_t queue_;

    if (queue.empty())
        queue_ = amqp_empty_bytes;
    else
        queue_ = amqp_cstring_bytes(queue.c_str());

    amqp_table_t table = amqp_empty_table;
    if (args && args->getEntriesSize() > 0)
    {
        table.num_entries = args->getEntriesSize();
        table.entries = (amqp_table_entry_t_*)args->getEntries();
    }

    amqp_queue_declare_ok_t* r = amqp_queue_declare(_tcpConn.connnection(), _channelId, queue_,
                                                    passive, durable, exclusive, autoDel, table);

    if (!r)
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Declare queue failed", res);
    }

    std::string strQueue = std::string((char*)r->queue.bytes, r->queue.len);
    return strQueue;
}

void AMQP::Channel::bindQueue(const std::string& exchange, const std::string& queue,
                              const std::string& bindingkey)
{
    amqp_queue_bind_ok_t* r =
        amqp_queue_bind(_tcpConn.connnection(), _channelId, amqp_cstring_bytes(queue.c_str()),
                        amqp_cstring_bytes(exchange.c_str()),
                        amqp_cstring_bytes(bindingkey.c_str()), amqp_empty_table);
    if (!r)
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Bind queue failed", res);
    }
}

void AMQP::Channel::unbindQueue(const std::string& exchange, const std::string& queue,
                                const std::string& bindingkey)
{

    amqp_queue_unbind_ok_t* r =
        amqp_queue_unbind(_tcpConn.connnection(), _channelId, amqp_cstring_bytes(queue.c_str()),
                          amqp_cstring_bytes(exchange.c_str()),
                          amqp_cstring_bytes(bindingkey.c_str()), amqp_empty_table);
    if (!r)
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Unbind queue failed", res);
    }
}

void AMQP::Channel::close()
{
    if (isOpened())
    {
        amqp_channel_close(_tcpConn.connnection(), _channelId, AMQP_REPLY_SUCCESS);
        updateChannelState(CLOSED);
    }
}

void AMQP::Channel::open(uint16_t channel /*= 1*/)
{
    if (!amqp_channel_open(_tcpConn.connnection(), _channelId))
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Open channel failed", res);
    }
    updateChannelState(ChannelState::USING);
}

AMQP::Channel::~Channel()
{
    if (isOpened()) close();
}

std::string AMQP::Channel::basicConsume(amqp_bytes_t queue, const std::string& consumerTag,
                                        bool bNoLocal /*= false*/, bool bNoAck /*= false*/,
                                        bool bExclusive /*= false*/, const Table* args)
{
    amqp_connection_state_t conn = _tcpConn.connnection();
    amqp_table_t table = amqp_empty_table;

    if (args && args->getEntriesSize() > 0)
    {
        table.num_entries = args->getEntriesSize();
        table.entries = (amqp_table_entry_t_*)args->getEntries();
    }

    amqp_bytes_t csmTag;
    if (consumerTag.empty())
        csmTag = amqp_empty_bytes;
    else
        csmTag = amqp_cstring_bytes(consumerTag.c_str());
    amqp_basic_consume_ok_t* r =
        amqp_basic_consume(conn, _channelId, queue, csmTag, bNoLocal, bNoAck, bExclusive, table);

    if (!r)
    {
        auto res = amqp_get_rpc_reply(_tcpConn.connnection());
        throw AMQP::Exception("Consume on queue failed", res);
    }

    return std::string((char*)(r->consumer_tag.bytes), r->consumer_tag.len);
}

void AMQP::Channel::assertConnection()
{
    if (!_tcpConn.isLogined()) throw AMQP::Exception("No connection.");
}

void AMQP::Channel::dumpBasicProps(const MessageProps* pProps, amqp_basic_properties_t& pAmqpProps)
{
    if (pProps == nullptr) return;

    if (!pProps->replyQueue.empty())
    {
        pAmqpProps._flags |= AMQP_BASIC_REPLY_TO_FLAG;
        pAmqpProps.reply_to = amqp_bytes_malloc_dup(amqp_cstring_bytes(pProps->replyQueue.c_str()));
        // m_bExitsReplyTo = true;
    }

    if (!pProps->correlationId.empty())
    {
        pAmqpProps._flags |= AMQP_BASIC_CORRELATION_ID_FLAG;
        pAmqpProps.correlation_id = amqp_cstring_bytes(pProps->correlationId.c_str());
    }

    if (!pProps->msgId.empty())
    {
        pAmqpProps._flags |= AMQP_BASIC_MESSAGE_ID_FLAG;
        pAmqpProps.message_id = amqp_cstring_bytes(pProps->msgId.c_str());
    }

    if (pProps->deliveryMode > 0) pAmqpProps.delivery_mode = pProps->deliveryMode;

    if (!pProps->contentType.empty())
        pAmqpProps.content_type = amqp_cstring_bytes(pProps->contentType.c_str());
}

void AMQP::Channel::initBasicProps(amqp_basic_properties_t& pAmqpProps)
{
    pAmqpProps._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
    pAmqpProps.content_type = amqp_cstring_bytes("text/plain");
    pAmqpProps.correlation_id = amqp_empty_bytes;
    pAmqpProps.delivery_mode = 2; /* persistent delivery mode */
}

void AMQP::Channel::updateChannelState(ChannelState state)
{
    _tcpConn._channelsState.insert_or_assign(this->_channelId, state);
}

AMQP::Channel::Channel(TCPConnection& conn, uint16_t id) : _tcpConn(conn), _channelId(id)
{
    open(_channelId);
}

AMQP::Table::Table() { _entries = std::make_shared<std::vector<amqp_table_entry_t_>>(); }

AMQP::Table::Table(Table&& other) noexcept { *this = std::move(other); }

AMQP::Table::Table(const Table& other) { *this = other; }

Table& AMQP::Table::operator=(Table&& other) noexcept
{
    if (this != &other)
    {
        empty();
        this->_entries = other._entries;
        other._entries = std::make_shared<Entries>();
    }
    return *this;
}

void AMQP::Table::empty()
{
    if (_entries && _entries->size())
    {
        for (auto it = _entries->begin(); it != _entries->end(); it++)
        {
            if (it->value.kind == AMQP_FIELD_KIND_BYTES) amqp_bytes_free(it->value.value.bytes);
            amqp_bytes_free(it->key);
        }
    }
    _entries = std::make_shared<Entries>();
}

Table& AMQP::Table::operator=(const Table& other)
{

    if (this != &other)
    {
        empty();

        for (auto it = other._entries->begin(); it != other._entries->end(); it++)
        {
            switch (it->value.kind)
            {
            case AMQP_FIELD_KIND_BYTES:
            {
                addEntry((const char*)it->key.bytes, (const char*)it->value.value.bytes.bytes);
            }
            break;
            case AMQP_FIELD_KIND_BOOLEAN:
            {
                addEntry((const char*)it->key.bytes, it->value.value.boolean);
            }
            break;
            case AMQP_FIELD_KIND_F32:
            {
                addEntry((const char*)it->key.bytes, it->value.value.f32);
            }
            break;
            case AMQP_FIELD_KIND_F64:
            {
                addEntry((const char*)it->key.bytes, it->value.value.f64);
            }
            break;
            case AMQP_FIELD_KIND_I8:
            {
                addEntry((const char*)it->key.bytes, it->value.value.i8);
            }
            break;
            case AMQP_FIELD_KIND_U8:
            {
                addEntry((const char*)it->key.bytes, it->value.value.u8);
            }
            break;
            case AMQP_FIELD_KIND_I16:
            {
                addEntry((const char*)it->key.bytes, it->value.value.i16);
            }
            break;
            case AMQP_FIELD_KIND_U16:
            {
                addEntry((const char*)it->key.bytes, it->value.value.u16);
            }
            break;
            case AMQP_FIELD_KIND_I32:
            {
                addEntry((const char*)it->key.bytes, it->value.value.i32);
            }
            break;
            case AMQP_FIELD_KIND_U32:
            {
                addEntry((const char*)it->key.bytes, it->value.value.u32);
            }
            break;
            default:
                break;
            }
        }
    }
    return *this;
}

void AMQP::Table::addEntry(const char* key, const char* value)
{
    if (_entries == nullptr) return;

    amqp_table_entry_t entry;

    entry.key = amqp_bytes_malloc_dup(amqp_cstring_bytes(key));
    entry.value.kind = AMQP_FIELD_KIND_BYTES;
    entry.value.value.bytes = amqp_bytes_malloc_dup(amqp_cstring_bytes(value));

    _entries->push_back(entry);
}

void AMQP::Table::addEntry(const char* key, bool value)
{
    if (_entries == nullptr) return;

    amqp_table_entry_t entry;

    entry.key = amqp_bytes_malloc_dup(amqp_cstring_bytes(key));
    entry.value.kind = AMQP_FIELD_KIND_BOOLEAN;
    entry.value.value.boolean = (amqp_boolean_t)value;

    _entries->push_back(entry);
}

void AMQP::Table::addEntry(const char* key, float value)
{
    if (_entries == nullptr) return;

    amqp_table_entry_t entry;

    entry.key = amqp_bytes_malloc_dup(amqp_cstring_bytes(key));
    entry.value.kind = AMQP_FIELD_KIND_F32;
    entry.value.value.f32 = value;

    _entries->push_back(entry);
}

void AMQP::Table::addEntry(const char* key, double value)
{
    if (_entries == nullptr) return;

    amqp_table_entry_t entry;

    entry.key = amqp_bytes_malloc_dup(amqp_cstring_bytes(key));
    entry.value.kind = AMQP_FIELD_KIND_F64;
    entry.value.value.f64 = value;

    _entries->push_back(entry);
}

void AMQP::Table::addEntry(const char* key, std::int8_t value)
{
    if (_entries == nullptr) return;

    amqp_table_entry_t entry;

    entry.key = amqp_bytes_malloc_dup(amqp_cstring_bytes(key));
    entry.value.kind = AMQP_FIELD_KIND_I8;
    entry.value.value.i8 = value;

    _entries->push_back(entry);
}

void AMQP::Table::addEntry(const char* key, std::uint8_t value)
{
    if (_entries == nullptr) return;

    amqp_table_entry_t entry;

    entry.key = amqp_bytes_malloc_dup(amqp_cstring_bytes(key));
    entry.value.kind = AMQP_FIELD_KIND_U8;
    entry.value.value.u8 = value;

    _entries->push_back(entry);
}

void AMQP::Table::addEntry(const char* key, std::int16_t value)
{
    if (_entries == nullptr) return;

    amqp_table_entry_t entry;

    entry.key = amqp_bytes_malloc_dup(amqp_cstring_bytes(key));
    entry.value.kind = AMQP_FIELD_KIND_I16;
    entry.value.value.i16 = value;

    _entries->push_back(entry);
}

void AMQP::Table::addEntry(const char* key, std::uint16_t value)
{
    if (_entries == nullptr) return;

    amqp_table_entry_t entry;

    entry.key = amqp_bytes_malloc_dup(amqp_cstring_bytes(key));
    entry.value.kind = AMQP_FIELD_KIND_U16;
    entry.value.value.u16 = value;

    _entries->push_back(entry);
}

void AMQP::Table::addEntry(const char* key, std::int32_t value)
{
    if (_entries == nullptr) return;

    amqp_table_entry_t entry;

    entry.key = amqp_bytes_malloc_dup(amqp_cstring_bytes(key));
    entry.value.kind = AMQP_FIELD_KIND_I32;
    entry.value.value.i32 = value;

    _entries->push_back(entry);
}

void AMQP::Table::addEntry(const char* key, std::uint32_t value)
{
    if (_entries == nullptr) return;

    amqp_table_entry_t entry;

    entry.key = amqp_bytes_malloc_dup(amqp_cstring_bytes(key));
    entry.value.kind = AMQP_FIELD_KIND_U32;
    entry.value.value.u32 = value;

    _entries->push_back(entry);
}

void AMQP::Table::removeEntry(const char* key)
{
    if (_entries && _entries->size())
    {
        for (auto it = _entries->begin(); it != _entries->end(); it++)
        {
            if (std::string((char*)it->key.bytes, it->key.len) == key)
            {
                if (it->value.kind == AMQP_FIELD_KIND_BYTES) amqp_bytes_free(it->value.value.bytes);
                amqp_bytes_free(it->key);

                _entries->erase(it);
                break;
            }
        }
    }
}
