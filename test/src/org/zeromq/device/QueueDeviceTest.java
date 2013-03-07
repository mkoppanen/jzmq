package org.zeromq.device;

import junit.framework.TestCase;

import org.junit.Test;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQQueue;

public class QueueDeviceTest extends TestCase {
    @Test
    public void testQueueDeviceXPUBXSUBProxy() throws InterruptedException {
        ZContext context = new ZContext();
        context.setContext(ZMQ.context(1));

        Socket backend = context.createSocket(ZMQ.XPUB);
        backend.bind("ipc:///tmp/proxy");

        Socket frontend = context.createSocket(ZMQ.XSUB);
        frontend.connect("ipc:///tmp/work");

        // QueueDevice queue = new QueueDevice(context, frontend, backend);
        // queue.start();
        ZMQQueue queue = new ZMQQueue(context.getContext(), frontend, backend);
        // assertEquals("Queue should be started", true, queue.isStarted());

        Socket pub = context.createSocket(ZMQ.PUB);
        pub.bind("ipc:///tmp/work");

        Socket sub = context.createSocket(ZMQ.SUB);
        sub.subscribe("A".getBytes());
        sub.connect("ipc:///tmp/proxy");

        Thread.sleep(100);
        pub.send("A".getBytes(), ZMQ.SNDMORE);
        pub.send("morecowbell");

        Poller poller = context.getContext().poller(1);
        poller.register(sub, ZMQ.Poller.POLLIN);
        try {
            poller.poll(1000);
            if (poller.pollin(0)) {
                assertEquals("morecowbell", new String(sub.recv()));
            } else {
                fail("Should have received a message");
            }
        } finally {
            // queue.shutdown();
            sub.close();
            pub.close();
            // context.destroy();
        }
    }
}
