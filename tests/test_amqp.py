import aiormq
import asyncio
import decorator

@decorator.decorator
def asyncloop_run(f, asyncloop, *a, **kw):
    asyncloop.run(f(asyncloop, *a, **kw))

async def amqp_prepare():
    conn = await aiormq.connect('amqp://guest:guest@localhost/')
    chan = await conn.channel()

    amqp_queue = (await chan.queue_declare('', auto_delete=True)).queue

    return conn, chan, amqp_queue

@asyncloop_run
async def test_pub(asyncloop):
    conn, chan, amqp_queue = await amqp_prepare()
    queue = asyncio.Queue()
    async def consume(m):
        await queue.put(m)

    await chan.basic_consume(amqp_queue, consume, no_ack=True)

    pub = asyncloop.Channel('amqp://localhost:5672', queue=amqp_queue, name='amqp', dump='frame')
    pub.open()
    assert (await pub.recv_state()) == pub.State.Active

    for i in range(3):
        pub.post(b'xxx-%d' % i)

    for i in range(3):
        m = await queue.get()
        assert m.body == b'xxx-%d' % i

@asyncloop_run
async def test_sub(asyncloop):
    conn, chan, amqp_queue = await amqp_prepare()

    c = asyncloop.Channel('amqp://localhost:5672', queue=amqp_queue, mode='sub', name='amqp', dump='frame')
    c.open()
    assert (await c.recv_state()) == c.State.Active

    for i in range(3):
        await chan.basic_publish(b'xxx-%d' % i, routing_key=amqp_queue)

    for i in range(3):
        m = await c.recv()
        assert m.data.tobytes() == b'xxx-%d' % i
