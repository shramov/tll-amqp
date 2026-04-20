// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Pavel Shramov <shramov@mexmat.net>

#include <tll/channel/base.h>
#include <tll/util/time.h>
#include <tll/util/scoped_fd.h>
#include <tll/util/pointer_list.h>

#include <amqpcpp/flags.h>
#include <amqpcpp/linux_tcp.h>

class TLLHandler;

class Event : public tll::channel::Base<Event>
{
 protected:
	using Base = tll::channel::Base<Event>;

	AMQP::TcpConnection * _conn = nullptr;

 public:
	static constexpr std::string_view channel_protocol() { return "amqp-ev"; }
	static constexpr auto process_api_version() { return Base::ProcessAPI::Flags; }
	static constexpr auto process_flags_policy() { return Base::ProcessFlagsPolicy::PollHint; }

	void reset(AMQP::TcpConnection * conn, int fd)
	{
		_conn = conn;
		_update_fd(fd);
	}

	void update(int flags)
	{
		unsigned dcaps = 0;
		if (flags & AMQP::readable)
			dcaps |= tll::dcaps::CPOLLIN;
		if (flags & AMQP::writable)
			dcaps |= tll::dcaps::CPOLLOUT;
		this->_update_dcaps(dcaps, tll::dcaps::CPOLLMASK);
	}

	int _process(unsigned flags)
	{
		int aflags = 0;
		if (flags & TLL_PROCESS_READ)
			aflags |= AMQP::readable;
		if (flags & TLL_PROCESS_WRITE)
			aflags |= AMQP::writable;
		_conn->process(fd(), aflags);
		return 0;
	}
};

TLL_DEFINE_IMPL(Event);

class TLLHandler : public AMQP::TcpHandler
{
	std::map<int, std::unique_ptr<tll::Channel>> fdmap;

 public:
	tll_channel_internal_t * internal = nullptr;

	virtual ~TLLHandler() = default;

	void reset()
	{
		fdmap.clear();
	}

	virtual void monitor(AMQP::TcpConnection *connection, int fd, int flags) override
	{
		if (flags == 0) {
			fdmap.erase(fd);
			return;
		}

		auto it = fdmap.find(fd);
		tll::Channel * c;
		if (it == fdmap.end()) {
			c = init_event(connection, fd);
		} else
			c = it->second.get();

		tll::channel_cast<Event>(c)->update(flags);
	}

	tll::Channel * init_event(AMQP::TcpConnection * conn, int fd)
	{
		tll::Logger _log(internal->logger);
		tll::Channel::Url curl;
		curl.proto("amqp+event");
		curl.set("tll.internal", "yes");
		curl.set("name", fmt::format("{}/event/{}", internal->name, fd));
		std::unique_ptr<tll::Channel> c{(tll::Channel *)tll_channel_new_url(internal->self->context, curl, nullptr, &Event::impl)};
		if (!c)
			return _log.fail(nullptr, "Failed to create event channel");
		tll::channel_cast<Event>(c.get())->reset(conn, fd);
		if (c->open())
			return _log.fail(nullptr, "Failed to open wrapper channel");
		if (auto r = tll_channel_internal_child_add(internal, c.get(), nullptr, 0))
			return _log.fail(nullptr, "Failed to add child channel {}: {}", tll_channel_name(c.get()), strerror(r));
		return fdmap.emplace(fd, std::move(c)).first->second.get();
	}
};
