#pragma once
#include "Protobuf.h"
#include <UUID/UuidGenerator.h>

std::queue<Protobuf::rchannel*> Protobuf::lst_channel;
int Protobuf::size = 1;
double Protobuf::m_timeout = 1000;

std::string Protobuf::exchangeName = "";
std::string Protobuf::queueName = "";
#ifdef DEBUG
int RBMQPool::count = 0 ;
#endif // DEBUG
std::string RBMQPool::host;
int RBMQPool::port;
std::string RBMQPool::user;
std::string RBMQPool::password;
std::string RBMQPool::routingKey;


std::mutex RBMQPool::mtx;
std::condition_variable RBMQPool::cv;

RBMQPool::~RBMQPool()
{
	while (!lst_channel.empty())
	{
		RBMQPool::rchannel* element = lst_channel.front();
		if (element->channel && element->channel->isOpened())
		{
			element->channel->basicCancel(element->conTag);
			element->channel->close(); 
			element->isConnecting = FALSE;
			if (element->cnn)
				element->cnn->disconnect();
		}
		delete element;
		lst_channel.pop();
	}


}
BOOL RBMQPool::createPool()
{
	for (int i = 0; i < size; i++)
	{
		RBMQPool::rchannel* k = new RBMQPool::rchannel;
		lst_channel.push(k);
	}
	return TRUE; 
}
BOOL RBMQPool::newConnection(RBMQPool::rchannel*& _rchannel)
{
	std::lock_guard<std::mutex> lk(mtx);

	if (!_rchannel) return FALSE; 
	if (_rchannel->isConnecting) return TRUE;
		
	try
	{
		_rchannel->cnn = AMQP::TCPConnection::createConnection(host, port);

		// create connection with properties
		{
			AMQP::Table table;
			table.addEntry("consume_connect", "consume_connect");
			_rchannel->cnn->login("/", user, password, &table);

			timeval timeOut{ 3, 0 };
			_rchannel->cnn->setRPCTimeOut(timeOut);
		}
		_rchannel->isConnecting = TRUE;
		return TRUE;
	}
	catch (const std::exception&)
	{

	}
	return FALSE;
}
BOOL RBMQPool::newChannel(RBMQPool::rchannel*& _rchannel)
{
	try
	{
		if (!newConnection(_rchannel)) return FALSE;

		if ( _rchannel )
			if (_rchannel->channel)
				if (_rchannel->channel->isOpened() ) return TRUE;
		
		_rchannel->channel = _rchannel->cnn->createChannel();
 		//AMQP::Channel::u_ptr channel2 = cnn->createChannel();
		_rchannel->channel->basicQos(10);											// perfect count 
		_rchannel->conTag = _rchannel->channel->basicConsume(queueName);

		return TRUE; 
	}
	catch (const std::exception& e)
	{
		std::cout << e.what() << std::endl;
		return FALSE; 
	}
}
void RBMQPool::addChannel(RBMQPool::rchannel* _rchannel)
{
	if (_rchannel == NULL) return;
	std::lock_guard<std::mutex> lk(mtx);
	if (lst_channel.size() == size) return;
	lst_channel.push(_rchannel);
	cv.notify_all();
}

RBMQPool::rchannel* RBMQPool::getChannel()
{
	std::unique_lock<std::mutex> lk(mtx);
	if (cv.wait_for(lk, m_timeout * 1ms, [] {return lst_channel.size() != 0;  }))
	{
		auto _rchannel = lst_channel.front();
		lst_channel.pop();
		return _rchannel;
	}
	else
	{
		return NULL;
	}
}
BOOL RBMQPool::sendMsg(std::string msg)
{
	try
	{
		RBMQPool::rchannel* _rchannel = getChannel(); 

		clock_t start = clock();
		while (!newChannel(_rchannel))
		{
			if (((double)(clock() - start)) / CLOCKS_PER_SEC >= 1)
			{
				addChannel(_rchannel); 
				return FALSE;
			}
			continue;
		}

		AMQP::MessageProps msgProps;
		msgProps.msgId = VBD::UUID::Generate();

		// send text
		{
			_rchannel->channel->basicPublish(exchangeName, routingKey, msg, &msgProps);
		}

		return TRUE;
	}
	catch (const std::exception&)
	{
		//isConnecting = FALSE; 
		return FALSE; 
	}
}
BOOL RBMQPool::sendByte(std::string buffer)
{
	try
	{
		RBMQPool::rchannel* _rchannel = getChannel();

		clock_t start = clock();
		while (!newChannel(_rchannel))
		{
			if (((double)(clock() - start)) / CLOCKS_PER_SEC >= 1) 
			{
				addChannel(_rchannel);
				return FALSE;
			}
			continue;
		}
		{
			AMQP::MessageProps msgProps;
			msgProps.msgId = VBD::UUID::Generate();

			AMQP::Message msgSend;

			msgSend.len = buffer.size();
			msgSend.bytes = reinterpret_cast<BYTE*>(buffer.data()), buffer.size();

			_rchannel->channel->basicPublish(exchangeName, routingKey, msgSend, &msgProps);
		}

		return TRUE;
	}
	catch (const std::exception&)
	{
		//isConnecting = FALSE; 
		return FALSE;
	}

}


BOOL RBMQPool::receiveMsg(std::string& msg)
{
	try
	{
		RBMQPool::rchannel* _rchannel = getChannel();

		clock_t start = clock();
		while (!newChannel(_rchannel))
		{
			if (((double)(clock() - start)) / CLOCKS_PER_SEC >= 1) 
			{
				addChannel(_rchannel);
				return FALSE;
			}
			continue;
		}
		{

 			timeval timeOut = { 1, 0 };
			auto envelope = _rchannel->channel->getMessage(timeOut);
			//std::cout << " count msg : .." << channel->GetQueueMessageCount(); 
			//std::cout << "msg 10: " << envelope.msg.bytes << std::endl; 
			if (envelope.msg.bytes)
			{
#ifdef DEBUG
				count++;
#endif // DEBUG
				std::string text((char*)envelope.msg.bytes, envelope.msg.len);
				msg = text;
				//std::cout << "receive: " << msg << std::endl;
#ifdef DEBUG
				std::cout << "msg " << count << " : " << text << std::endl;
#endif // DEBUG
			}

			_rchannel->channel->basicAck(envelope, true);
		}
		addChannel(_rchannel);
		return TRUE;
	}
	catch (const std::exception&)
	{
		//isConnecting = FALSE; 
		return FALSE;
	}

}

BOOL RBMQPool::existMsg()
{
	try
	{
		RBMQPool::rchannel* _rchannel = getChannel();

		clock_t start = clock();
		while (!newChannel(_rchannel))
		{
			if (((double)(clock() - start)) / CLOCKS_PER_SEC >= 1) 
			{
				addChannel(_rchannel);
				return FALSE;
			}
			continue;
		}
		{

			timeval timeOut = { 1, 0 };
			auto envelope = _rchannel->channel->getMessage(timeOut);
			//std::cout << " count msg : .." << channel->GetQueueMessageCount(); 
			//std::cout << "msg 10: " << envelope.msg.bytes << std::endl; 
			if (envelope.msg.bytes)
			{
#ifdef DEBUG
				count++;
#endif // DEBUG
				std::string text((char*)envelope.msg.bytes, envelope.msg.len);
				addChannel(_rchannel);
				if (text != "") return TRUE;
				else return FALSE; 
				//std::cout << "receive: " << msg << std::endl;
#ifdef DEBUG
				std::cout << "msg " << count << " : " << text << std::endl;
#endif // DEBUG
			}
		}

		addChannel(_rchannel);
		return TRUE;
	}
	catch (const std::exception&)
	{
		//isConnecting = FALSE;
		return FALSE;
	}

}

BOOL RBMQPool::clearQueue()
{
	std::string buffer = "";

	try
	{
		RBMQPool::rchannel* _rchannel = getChannel();

		clock_t start = clock();
		while (!newChannel(_rchannel))
		{
			if (((double)(clock() - start)) / CLOCKS_PER_SEC >= 1)
			{
				addChannel(_rchannel);
				return FALSE;
			}
			continue;
		}

		if (_rchannel->channel)
			_rchannel->channel->purgeQueue(queueName);

		addChannel(_rchannel);
		return TRUE;
	}
	catch (const std::exception&)
	{
		return FALSE;
	}
	return TRUE;
}