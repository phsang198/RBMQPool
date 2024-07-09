//////////////////////////////////////////////////////////////////////////
// File: Exception.cpp
// Description: the implement of exception
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
#include "Exception.h"
#include <sstream>
using namespace AMQP;

AMQP::Exception::Exception()
{
    _message = "AMQP: unknown exception";
    _code = -1;
}

Exception::Exception(const std::string& message, int error_code)
{
    _message = "AMQP: " + message;
    if (error_code != -1)
    {
        _message += "\nDetails: ";
        _message += amqp_error_string2(error_code);
    }
    _code = error_code;
}

AMQP::Exception::Exception(const std::string& message, const amqp_rpc_reply_t& res)
{
    _message = "AMQP: " + message;
    std::string msg;
    replyToString(res, msg, _code);
    _message += "\nDetails: ";
    _message += msg;
}

int Exception::GetReplyCode() const { return _code; }

const char* AMQP::Exception::what() const throw() { return _message.c_str(); }

void AMQP::Exception::replyToString(const amqp_rpc_reply_t& res, std::string& msg, int& code)
{
    if (res.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION)
    {
        // msg = res.library_error ? strerror(res.library_error) : "end-of-stream";
        msg = amqp_error_string2(res.library_error);
        code = res.library_error;
    }
    else if (res.reply_type == AMQP_RESPONSE_SERVER_EXCEPTION)
    {
        char buf[512];
        memset(buf, 0, 512);
        code = 0;

        if (res.reply.id == AMQP_CONNECTION_CLOSE_METHOD)
        {
            amqp_connection_close_t* m = (amqp_connection_close_t*)res.reply.decoded;
            code = m->reply_code;

            sprintf(buf, "server connection error %uh, message: %.*s", m->reply_code,
                    (int)m->reply_text.len, (char*)m->reply_text.bytes);
        }
        else if (res.reply.id == AMQP_CHANNEL_CLOSE_METHOD)
        {
            amqp_channel_close_t* n = (amqp_channel_close_t*)res.reply.decoded;
            code = n->reply_code;

            sprintf(buf, "server channel error %d, message: %.*s class=%d method=%d", n->reply_code,
                    (int)n->reply_text.len, (char*)n->reply_text.bytes, (int)n->class_id,
                    n->method_id);
        }
        else
        {
            sprintf(buf, "unknown server error, method id 0x%08X", res.reply.id);
        }
        msg = buf;
    }
}
