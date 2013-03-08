package org.zeromq.device;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

public class QueueDevice {
    public static final long DEfAULT_POLL_TIMEOUT = 100;
    private final ExecutorService exec;
    private final ZContext context;
    private final Socket frontend;
    private final Socket backend;
    private final Poller poller;
    private final long pollTimeout;

    private enum State {
        LATENT, STARTED, CLOSED;
    }

    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    public QueueDevice(ZContext context, Socket frontend, Socket backend) {
        this(context, frontend, backend, DEfAULT_POLL_TIMEOUT);
    }

    public QueueDevice(ZContext context, Socket frontend, Socket backend, long pollTimeout) {
        this.context = context;
        this.frontend = frontend;
        this.backend = backend;
        this.poller = new Poller(2);
        this.pollTimeout = pollTimeout;
        this.exec = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setName("zeromq-queue");
                return t;
            }
        });
    }

    public void start() {
        if (!state.compareAndSet(State.LATENT, State.STARTED)) {
            throw new IllegalStateException("Queue device can only be started once");
        }
        exec.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                mainLoop();
                return null;
            }
        });
    }

    public boolean isStarted() {
        return state.get() == State.STARTED;
    }

    public boolean isClosed() {
        return state.get() == State.CLOSED;
    }

    public void mainLoop() {
        try {
            byte[] msg = null;
            boolean more = false;

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    if (poller.poll(pollTimeout) < 0) {
                        continue;
                    }
                    if (poller.pollin(0)) {
                        more = true;
                        do {
                            msg = frontend.recv(0);
                            more = frontend.hasReceiveMore();
                            if (msg != null) {
                                backend.send(msg, more ? ZMQ.SNDMORE : 0);
                            }
                        } while (more);
                    }
                    if (poller.pollin(1)) {
                        more = true;
                        do {
                            msg = backend.recv(0);
                            more = backend.hasReceiveMore();
                            if (msg != null) {
                                frontend.send(msg, more ? ZMQ.SNDMORE : 0);
                            }
                        } while (more);
                    }
                } catch (ZMQException e) {
                    if (ZMQ.Error.ETERM.getCode() == e.getErrorCode()) {
                        break;
                    }
                    throw e;
                }
            }
        } finally {
            frontend.close();
            backend.close();
        }
    }

    public void shutdown() {
        if (state.compareAndSet(State.STARTED, State.CLOSED)) {
            exec.shutdown();
            context.destroy();
        }
    }
}
