// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Pavel Shramov <shramov@mexmat.net>

#include <tll/channel/base.h>
#include <tll/channel/module.h>
#include <tll/util/hostport.h>

#include <amqpcpp.h>
#include <amqpcpp/linux_tcp.h>

#include "handler.h"

class AMQPConn : public tll::channel::Base<AMQPConn>
{
	using Base = tll::channel::Base<AMQPConn>;

	TLLHandler _handler;

	std::optional<AMQP::TcpChannel> _channel;

	std::string _exchange;
	std::string _key;

	std::optional<AMQP::TcpConnection> _conn;
	std::optional<AMQP::Address> _addr;

	bool _pub = true;
 public:
	static constexpr std::string_view channel_protocol() { return "amqp"; }
	static constexpr auto open_policy() { return Base::OpenPolicy::Manual; }

	int _init(const tll::Channel::Url &, tll::Channel *master);
	int _open(const tll::ConstConfig &);
	int _close();

	int _post(const tll_msg_t * msg, int flags)
	{
		if (!_pub)
			return ENOSYS;
		_channel->publish(_exchange, _key, (const char *) msg->data, msg->size, 0);
		return 0;
	}
};

int AMQPConn::_init(const tll::Channel::Url &cfg, tll::Channel *master)
{
	_handler.internal = &internal;

	auto reader = channel_props_reader(cfg);
	_exchange = reader.getT("exchange", std::string {});
	_key = reader.getT<std::string>("queue");
	_pub = reader.getT("mode", true, {{"pub", true}, {"sub", false}});
	auto vhost = reader.getT("vhost", std::string("/"));
	auto host = reader.getT<tll::network::hostport>("tll.host");
	if (!reader)
		return _log.fail(EINVAL, "Invalid init parameters: {}", reader.error());

	if (host.port == 0)
		host.port = 5672;
	_addr = AMQP::Address(host.host, host.port, AMQP::Login("guest", "guest"), vhost, false);
	return Base::_init(cfg, master);
}

int AMQPConn::_open(const tll::ConstConfig &cfg)
{
	if (auto r = Base::_open(cfg); r)
		return r;
	_conn.emplace(&_handler, *_addr);
	_channel.emplace(&*_conn);
	if (!_pub) {
		_log.debug("Declare consumer");
		_channel->consume(_key)
			.onSuccess([this](const std::string &tag) {
				_log.info("Consumer created for '{}'", tag);
				state(tll::state::Active);
			}).onReceived([this](const AMQP::Message &msg, uint64_t tag, bool redelivered) {
				tll_msg_t m = { TLL_MESSAGE_DATA };
				m.data = msg.body();
				m.size = msg.bodySize();
				_channel->ack(tag);
				_callback_data(&m);
			}).onError([this](const char * error) {
				_log.error("Consumer error: {}", error);
			});
	} else
		state(tll::state::Active);
	return 0;
}

int AMQPConn::_close()
{
	_channel.reset();
	_conn.reset();
	_handler.reset();
	return Base::_close();
}

TLL_DEFINE_IMPL(AMQPConn);
TLL_DEFINE_MODULE(AMQPConn);
