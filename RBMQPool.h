#pragma once
#include <iostream>
#include <string>
#include "RabbitMQWrapper/Channel.h"
#include "RabbitMQWrapper/Exception.h"
#include <winsock2.h>
#include <condition_variable>
#include <chrono>
#include <atomic>

using namespace std::chrono_literals;

class RBMQPool
{
public:
	RBMQPool() {}
	~RBMQPool();
	static BOOL createPool();

private:
	static std::mutex mtx;
	static std::condition_variable cv;
public:
	static std::string host;
	static int port; 
	static std::string user;
	static std::string password;
	static std::string routingKey;

	static std::string exchangeName;
	static std::string queueName;
private:
	struct rchannel
	{
		BOOL isConnecting = FALSE;
		AMQP::TCPConnection::ptr cnn;
		AMQP::Channel::u_ptr channel = nullptr;
		std::string conTag = "";
	};
	static int size;
	static double m_timeout;
	static std::queue<rchannel*> lst_channel;
	
#ifdef DEBUG
	static int count;
#endif // DEBUG

private:
	static BOOL newConnection(RBMQPool::rchannel*& _rchannel);

	static BOOL newChannel(RBMQPool::rchannel*& _rchannel);

	static void addChannel(RBMQPool::rchannel* _rchannel);
	static RBMQPool::rchannel* getChannel();
public:
	static BOOL sendMsg(std::string msg);
	static BOOL sendByte(std::string buffer);

	static BOOL receiveMsg(std::string& msg);

	static BOOL existMsg();

	static BOOL clearQueue();
};