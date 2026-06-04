package com.apm.benchmark;

import org.jctools.queues.MpscArrayQueue;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 측정 1. lock 큐(ArrayBlockingQueue)와 lock-free 큐(MpscArrayQueue)의 offer 지연을,
 * 공급을 천천히 올려가며 임계점까지 비교한다.
 *
 * <p>목적은 우위 증명이 아니라, 이론적으로 더 가볍다고 본 lock-free 큐가 agent의 실제
 * 동작점에서도 더 가벼운지 확인하고, 단일 consumer가 공급을 못 따라가기 시작하는 임계점이
 * 어디인지 찾고, 이 환경에서 측정이 닿지 못하는 한계를 파악하는 것이다.
 *
 * <p>구조는 agent를 그대로 따른다. agent는 타깃 앱의 요청 스레드들이 offer하고(멀티
 * producer), 단일 QueueWorker가 drain한다(단일 consumer). 그래서 부하는 @Setup에서 띄운
 * 스레드풀이 만든다. 풀의 각 스레드는 offer 한 번 뒤 {@link LockSupport#parkNanos(long)}로
 * 잠깐 코어를 놓아, 실제 요청 스레드가 대부분의 시간을 대기로 보내는 I/O 바운드 동작을
 * 흉내 낸다. 코어를 점유하지 않으므로 풀 크기(producerThreads)를 코어 수 너머까지 올려
 * 공급을 키울 수 있다.
 *
 * <p>consumer는 백그라운드 단일 스레드가 drain으로 큐를 비운다. 공급이 소비보다 적으면
 * 큐는 거의 빈 정상 상태로 유지된다. 공급을 올리다 단일 consumer가 못 따라가는 지점에서
 * 큐가 차며 offer가 실패하기 시작한다. 그 실패 횟수(dropped)가 임계점의 신호다.
 *
 * <p>측정 대상은 @Threads(1)로 고정한 단일 스레드의 탐침 offer 한 번이다. 배경 부하
 * 속에서 offer 한 번이 얼마나 걸리는지를 AverageTime(ns/op)으로 잰다. 부하 생성과 측정을
 * 분리해, 측정 스레드 자신이 부하의 일부가 되지 않도록 했다.
 *
 * <p>임계점 신호는 둘이다. dropped가 0(또는 거의 0)이다가 늘기 시작하는 지점, 그리고 탐침
 * offer 지연이 낮고 안정적이다가 꺾이는 지점이다. 가장 분명한 신호는 dropped다. 큐가
 * 가득 차면 offer는 빠르게 실패로 돌아가므로, 임계 이상에서 탐침 지연은 오히려 빨라질 수
 * 있어 단독 해석은 피한다. (배경 producer가 parkNanos로 일부러 코어를 놓으므로 -prof jfr의
 * park 이벤트에는 의도적 휴식이 섞여, park는 더 이상 임계 신호로 쓰지 않는다.)
 *
 * <p>한계: 배경 producer는 park로 코어를 놓으므로, 상시 코어를 점유하는 것은 consumer와
 * 탐침 둘뿐이다. 그만큼 코어 경쟁이 줄어 임계점을 더 큰 풀 크기까지 좇을 수 있다. 다만
 * 임계점에서의 공급이 너무 커 다수 producer가 동시에 깨어나는 구간에서는 여전히 코어
 * 경쟁이 측정에 섞일 수 있다. 또 offer 사이 휴식(PARK_NANOS)에 따라 임계점이 어느 풀
 * 크기에 떨어질지 달라지므로, 첫 실행 뒤 이 값으로 임계점을 측정 구간 안으로 들인다.
 *
 * <p>실행: 스윕은 producerThreads 파라미터로 자동으로 돈다(-t는 @Threads로 1에 고정).
 * 이 설정은 무릎 위치를 찾는 탐색용으로 fork와 iteration을 낮춰 두었다. 무릎을 찾은 뒤
 * 그 근처 풀 크기만 @Fork(2), iteration을 올려 정밀하게 다시 잰다.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@Fork(1)
@Threads(1)
public class LatencyBenchmark {

    @Param({"ARRAY_BLOCKING", "MPSC_ARRAY"})
    public String queueType;

    /** 공급을 천천히 올리는 스윕 손잡이. 동시 producer(= 타깃 앱 요청 스레드) 수.
     *  배경이 park로 코어를 놓으므로 코어 수 너머까지 띄울 수 있다. */
    @Param({"1", "2", "4", "8", "16", "32", "64"})
    public int producerThreads;

    /** 배경 producer의 offer 사이 평균 휴식(ns). 공급 속도 손잡이 —
     *  줄이면 공급이 빨라져 임계점이 더 작은 풀 크기로 내려온다. */
    private static final long PARK_NANOS = 1000;

    Queue<QueueItem> queue;
    QueueItem sample;

    private Thread consumerThread;
    private final AtomicBoolean consumerRunning = new AtomicBoolean(false);

    private ExecutorService producerPool;
    private final AtomicBoolean producersRunning = new AtomicBoolean(false);

    /** 큐가 가득 차 offer가 실패한 횟수. 단일 consumer가 공급을 못 따라가는 임계 신호. */
    private final AtomicLong dropped = new AtomicLong();

    @Setup(Level.Trial)
    public void setup() {
        if ("ARRAY_BLOCKING".equals(queueType)) {
            queue = new ArrayBlockingQueue<>(1000);
        } else {
            queue = new MpscArrayQueue<>(1000);
        }
        sample = new QueueItem(DataType.SPAN, new Object());
        dropped.set(0);

        // 단일 consumer — agent의 QueueWorker(단일 스레드)와 동일. 큐를 계속 비운다.
        consumerRunning.set(true);
        consumerThread = new Thread(this::consume, "bench-consumer");
        consumerThread.setDaemon(true);
        consumerThread.start();

        // 배경 부하 — 타깃 앱 요청 스레드(멀티 producer)에 대응. 풀 크기로 공급을 올린다.
        producersRunning.set(true);
        if (producerThreads > 0) {
            producerPool = Executors.newFixedThreadPool(producerThreads);
            for (int i = 0; i < producerThreads; i++) {
                producerPool.submit(this::produceLoad);
            }
        }
    }

    /** 배경 producer 한 명. offer 한 번 뒤 parkNanos로 잠깐 코어를 놓아, 실제 요청 스레드처럼
     *  I/O 바운드로 동작한다(띄엄띄엄 offer). */
    private void produceLoad() {
        while (producersRunning.get()) {
            if (!queue.offer(sample)) {
                dropped.incrementAndGet();
            }
            // 요청 처리 사이 간격을 흉내. 랜덤 폭은 스레드들이 동시에 깨어 몰리는 것을 줄인다.
            LockSupport.parkNanos(
                    ThreadLocalRandom.current().nextLong(PARK_NANOS / 2, PARK_NANOS * 3 / 2));
        }
    }

    private void consume() {
        List<QueueItem> sink = new ArrayList<>(100);
        while (consumerRunning.get()) {
            drain(queue, sink, 100);
            sink.clear();
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws InterruptedException {
        producersRunning.set(false);
        if (producerPool != null) {
            producerPool.shutdown();
            producerPool.awaitTermination(1, TimeUnit.SECONDS);
        }
        consumerRunning.set(false);
        consumerThread.join(1000);

        System.out.println("[drop] " + queueType + " producers=" + producerThreads
                + " dropped=" + dropped.get());
    }

    /**
     * 측정 대상. 배경 부하 속에서 offer 한 번의 지연을 잰다. @Threads(1)로 이 메서드를 도는
     * 단일 측정 스레드가 탐침이며, 배경 부하(스레드풀)와 분리돼 있다.
     */
    @Benchmark
    public void offer(Blackhole bh) {
        bh.consume(queue.offer(sample));
    }

    /**
     * 큐 종류에 맞는 소비. agent DataQueueImpl과 동일하게 MpscArrayQueue는 drain,
     * ArrayBlockingQueue는 drainTo를 쓴다.
     */
    private static void drain(Queue<QueueItem> q, List<QueueItem> sink, int limit) {
        if (q instanceof MpscArrayQueue<QueueItem> mpsc) {
            mpsc.drain(sink::add, limit);
        } else if (q instanceof ArrayBlockingQueue<QueueItem> abq) {
            abq.drainTo(sink, limit);
        }
    }
}
