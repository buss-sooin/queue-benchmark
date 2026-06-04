# 큐 자료구조 선택과 적재 지연 측정

## 큐의 역할과 제한 조건

이 프로젝트의 agent는 모니터링 대상 앱 하나마다 하나씩 붙어 앱 프로세스(JVM) 안에서 동작합니다. 앱의 여러 요청 스레드가 후킹 지점을 지나며 수집한 span을 하나의 큐에 인입하고, 큐에서 span을 꺼내 gateway로 보내는 출구는 QueueWorker라는 단일 객체 하나입니다.

다수 producer와 단일 consumer가 한 큐를 공유하는 이 구조를 MPSC(Multi-Producer Single-Consumer)라고 합니다.

이 큐가 만족해야 할 제한 조건은 세 가지입니다.

- 앱 간섭 최소화: 큐에 넣는 동작은 모니터링 대상 앱의 요청 스레드가 직접 수행합니다. 이 동작이 느리면 앱의 요청 처리가 그만큼 지연됩니다. 모니터링 도구는 대상 앱에 주는 영향을 작게 유지해야 한다고 판단했고, 큐에 넣는 한 번의 지연을 줄이는 것이 여기에 직결됩니다.
- 드롭 허용: 모니터링 데이터는 앱의 정상 동작보다 우선순위가 낮습니다. 큐가 가득 차면 새로 들어오는 span을 버려 앱의 요청 처리를 막지 않습니다.
- 크기 제한(bounded): 메모리 사용에 상한을 두기 위해 큐 용량을 고정합니다. 드롭 허용은 이 고정 용량과 함께 동작합니다.

## 큐 자료구조 선택

수집한 데이터가 들어와 전송으로 빠져나가는 흐름에 맞는 Java 자료구조는 queue입니다. Java의 Queue 구현체에는 여러 종류가 있지만, 용량을 고정한 채 넣고 빼는 흐름이라는 조건으로 ArrayBlockingQueue와 LinkedBlockingQueue 두 개로 좁혔습니다. ArrayBlockingQueue는 넣기와 꺼내기가 lock 하나를 공유하고, LinkedBlockingQueue는 넣기와 꺼내기가 각각 별도의 lock으로 동작합니다. consumer가 QueueWorker 하나뿐이고 큐에 넣어 꺼내 전송하기만 하는 단순한 흐름에는 단일 lock의 ArrayBlockingQueue가 맞다고 보고 ArrayBlockingQueue로 구현을 시작했습니다.

큐는 agent가 선언해 들고 있는 객체이지만, 큐에 넣는 일은 타깃 앱의 요청 스레드가 직접 수행합니다. agent의 후킹 코드가 그 요청 스레드에서 함께 실행되기 때문입니다. ArrayBlockingQueue의 단일 lock에 경쟁이 생기면, lock을 얻으려 기다리는 주체가 바로 타깃 앱의 요청 스레드입니다. 기다리는 그 시간만큼 타깃 앱의 요청 처리가 늦어집니다. agent가 들고 있는 자료구조의 부하가 타깃 앱의 요청 처리 지연으로 그대로 넘어가는, agent 모듈만의 특수성입니다.

이 특수성을 줄이려고 lock 경쟁이 어디서 생기는지부터 갈라 봤습니다. 부하는 두 곳에서 생깁니다. 하나는 큐에서 꺼내 gateway로 보내는 전송 부하이고, 다른 하나는 후킹 데이터가 큐로 흘러드는 인입 부하입니다. 타깃 앱 안에서 도는 쪽은 인입 부하이므로, 타깃 앱에 직결되는 것은 넣는 쪽인 producer의 lock 경쟁입니다.

이 프로젝트에서는 producer 쪽 부하가 높지 않아 ArrayBlockingQueue로도 충분히 동작합니다. 다만 후킹 포인트를 크게 확장하면 한 요청을 처리하는 동안 큐에 넣는 횟수가 늘고, producer가 같은 lock을 두고 벌이는 경쟁도 함께 커집니다. 그 경쟁은 다시 타깃 앱의 요청 처리 지연으로 넘어갑니다.

producer 쪽 lock 경쟁을 없애려면 lock-free 큐가 필요합니다. JCTools의 MpscArrayQueue는 다수 producer와 단일 consumer(Multi-Producer Single-Consumer) 구조를 전제로 한 lock-free 큐이며, 용량도 고정할 수 있습니다. JDK가 제공하는 lock-free 큐인 ConcurrentLinkedQueue는 용량을 제한할 수 없어 용량을 고정해야 한다는 조건에 맞지 않으므로, 고정 용량과 lock-free를 함께 만족하는 MpscArrayQueue로 교체했습니다.

## 측정 지표와 방법

측정한 값은 agent의 후킹 데이터를 큐에 적재하는 데 걸리는 시간, 즉 적재 지연입니다.

이 지연을 지표로 삼은 이유는 앱 간섭에 있습니다. 큐에 적재하는 producer는 타깃 앱의 요청 스레드이고, 적재 한 건에 걸리는 시간이 그대로 타깃 앱의 요청 처리 시간에 더해집니다. 큐 성능을 재는 다른 지표인 throughput은 인입 데이터를 폭증시켜 큐의 임계 용량이 어디까지 버티는지를 봅니다. 그런데 큐로 들어오는 양은 타깃 앱의 호출 구조에 묶여 있어, 후킹 지점이 아무리 많아져도 agent 혼자서는 그 임계 용량까지 밀어붙이지 못합니다. throughput은 agent에서 일어나지 않는 상황을 재는 지표인 셈입니다.

적재 지연은 평균과 변동 두 가지로 봤습니다. 평균은 수많은 적재의 지연을 평균낸 값입니다. 변동은 그 지연들이 평균에서 얼마나 흩어져 있는지, 적재마다 시간이 얼마나 들쭉날쭉한지입니다. lock 큐에서는 적재 한 건이 lock 경쟁에 걸리면 그 한 건만 평균보다 훨씬 오래 걸립니다. 이렇게 개별 적재가 평균 위로 크게 벗어나는 것이 지연이 튀는 상태입니다. 모니터링 도구가 타깃 앱의 응답을 이렇게 가끔씩 튀게 만들면 평균이 낮아도 문제이므로, 변동을 평균과 함께 봤습니다.

테스트는 agent의 큐 구조를 최대한 그대로 옮겼습니다. agent에서 타깃 앱 요청 스레드가 큐에 데이터를 넣고 단일 worker가 비우는 구조를, 스레드풀이 데이터를 넣어 부하를 만들고 consumer 하나가 비우는 형태로 구현했습니다. 측정 메서드는 offer 호출 하나만 실행합니다. 스레드풀이 큐에 데이터를 계속 넣는 동안 offer가 한 번 실행되는 시간을 쟀고, 스레드풀 크기를 단계별로 늘려 가며 offer 지연이 어떻게 달라지는지 측정했습니다.

## 측정 결과와 한계

표의 ns/op은 offer 호출 1회의 평균 시간입니다. ± 뒤의 값은 반복 측정값 사이의 오차(99.9% 신뢰구간)입니다. 오차가 작으면 평균이 실제 값에 가까워 신뢰할 수 있고, 크면 실제 값에서 멀어 신뢰하기 어렵습니다. drop은 큐가 가득 찼을 때 이후 들어온 데이터를 버린 횟수입니다.

| producerThreads | ArrayBlockingQueue (ns/op) | MpscArrayQueue (ns/op) | ABQ drop | MPSC drop |
|---|---|---|---|---|
| 1 | 69.9 ± 35.6 | 25.3 ± 0.66 | 39,690 | 2,879 |
| 2 | 59.9 ± 14.7 | 23.2 ± 0.27 | 59,328 | 5,895 |
| 4 | 63.3 ± 8.8 | 21.2 ± 0.15 | 173,588 | 30,326 |
| 8 | 94.4 ± 6.2 | 22.5 ± 0.55 | 492,854 | 371,846 |
| 16 | 220.8 ± 15.9 | 12.5 ± 2.8 | 1,061,700 | 14,098,409 |
| 32 | 603.7 ± 34.9 | 54.5 ± 35.2 | 3,226,246 | 22,374,195 |
| 64 | 3297.5 ± 2976.3 | 52.1 ± 50.2 | 9,561,047 | 23,637,967 |

producerThreads 1부터 4까지는 단일 consumer가 공급을 따라가 큐가 가득 차지 않습니다. 이 구간에서 MpscArrayQueue의 offer 지연은 21\~25ns이고 오차가 ±0.15\~0.66ns로 작습니다. ArrayBlockingQueue는 60\~70ns로 세 배가량 느리고 오차도 ±8.8\~35.6ns로 큽니다. MpscArrayQueue가 적재 지연을 낮게 유지한 결과입니다.

producerThreads를 더 늘리면 consumer 하나가 공급을 따라가지 못해 큐가 가득 찹니다. MpscArrayQueue는 producerThreads 8까지 offer 지연이 일정하다가, 16에서 drop이 37만에서 1410만으로 급등합니다. 큐가 가득 차 offer가 바로 실패하는 횟수가 급증했다는 의미입니다. 이 구간에서 offer 지연은 12.5ns로 낮아지는데, 적재 연산을 하지 않고 바로 drop 하기 때문입니다.

ArrayBlockingQueue는 producerThreads가 늘수록 offer 지연이 계속 증가합니다. 8에서 94ns, 16에서 221ns, 32에서 604ns, 64에서 3298ns로 오릅니다. offer와 drain이 단일 lock을 공유하므로 producer가 늘면 lock 경쟁이 심해지고, 큐가 가득 찼는지 확인하는 데에도 lock을 먼저 얻어야 하기 때문에 이 비용이 offer 지연으로 나타납니다.

측정 메서드는 측정 시간 동안 offer를 쉴 새 없이 호출합니다. 배경 스레드도 쉴 새 없이 offer하면 큐가 곧장 가득 차 처리 속도를 잴 수 없습니다. 그래서 배경 스레드의 offer 사이에 유휴 시간을 줘 공급 속도에 완충을 뒀습니다. 이렇게 하면 producerThreads를 늘려 가며 큐가 가득 차는 지점을 찾고 그 전까지 처리 속도를 관측할 수 있습니다. 가득 차는 지점을 넘으면 오차가 커져, ArrayBlockingQueue producerThreads 64는 오차가 ±2976ns로 평균 3298ns만큼 커집니다. 이 수치는 신뢰할 수 없으므로 producerThreads 1부터 4까지를 분석 대상으로 삼았습니다.

분석 대상 구간의 지연 차이는 offer 한 번에 약 40ns입니다. 큐에 데이터를 넣는 일은 타깃 앱 위에서 도는 agent에서, 타깃 앱이 요청을 처리하는 동안 일어납니다. 요청 처리는 보통 밀리초 단위라, 큐 성능을 재는 나노초 단위의 차이는 실질적으로 체감하기 어렵습니다. 측정으로 얻은 사실은 MpscArrayQueue의 처리 지연이 작고 일정해, 다수 공급과 단일 소비 통로를 가진 구조에서 이점을 보인다는 점입니다.

## 쓴 도구와 라이브러리

측정에는 JMH(Java Microbenchmark Harness)를 썼습니다. 나노초 단위의 짧은 코드를 그냥 재면 JIT 컴파일이나 최적화가 끼어들어 시간이 왜곡됩니다. JMH는 warmup으로 JIT 컴파일을 안정시킨 뒤 측정하고, 측정을 여러 번 반복하며, 별도 JVM(fork)에서 실행해 이런 왜곡을 줄입니다. 표의 오차는 이 반복 측정에서 나온 값입니다.

MpscArrayQueue는 JCTools 라이브러리가 제공하는 lock-free 큐입니다. [JCTools 공식 문서](https://javadoc.io/static/org.jctools/jctools-core/3.3.0/org/jctools/queues/MpscArrayQueue.html)는 어느 스레드든 offer를 호출할 수 있지만 poll과 peek은 한 스레드만 호출해야 정확성이 유지된다고 명시합니다. 이것이 MPSC, 곧 다수 producer와 단일 consumer 구조와 동일합니다. 이 구조는 agent의 큐 구조와 같아, 이 사례에서 ArrayBlockingQueue보다 MpscArrayQueue가 더 적합한 자료구조라고 생각했습니다.

## 실행 방법

빌드는 jmhJar로 벤치마크 jar를 만듭니다.

```
./gradlew jmhJar
```

producerThreads가 @Param으로 1, 2, 4, 8, 16, 32, 64로 선언돼 있어, jar를 한 번 실행하면 모든 조합이 차례로 측정됩니다.

```
java -jar build/libs/queue-benchmark-jmh.jar LatencyBenchmark
```

실행하면 조합마다 warmup과 measurement iteration이 출력되고, 마지막 measurement 줄에 그 조합의 drop 수가 [drop]으로 붙습니다. 모든 조합이 끝나면 요약 표가 나오는데, 각 행이 producerThreads와 queueType 조합이고 Score가 offer 한 건의 평균 시간, Error가 99.9% 신뢰구간입니다.

```
# Parameters: (producerThreads = 1, queueType = MPSC_ARRAY)
Iteration   1: 25.391 ns/op
Iteration   2: 25.128 ns/op
Iteration   3: 25.579 ns/op
Iteration   4: 25.229 ns/op
Iteration   5: [drop] MPSC_ARRAY producers=1 dropped=2879
              25.369 ns/op

Result "com.apm.benchmark.LatencyBenchmark.offer":
  25.339 ±(99.9%) 0.661 ns/op [Average]

...

Benchmark               (producerThreads)     (queueType)  Mode  Cnt     Score      Error  Units
LatencyBenchmark.offer                  1  ARRAY_BLOCKING  avgt    5    69.854 ±   35.591  ns/op
LatencyBenchmark.offer                  1      MPSC_ARRAY  avgt    5    25.339 ±    0.661  ns/op
LatencyBenchmark.offer                  8  ARRAY_BLOCKING  avgt    5    94.443 ±    6.170  ns/op
LatencyBenchmark.offer                  8      MPSC_ARRAY  avgt    5    22.458 ±    0.546  ns/op
LatencyBenchmark.offer                 16  ARRAY_BLOCKING  avgt    5   220.773 ±   15.934  ns/op
LatencyBenchmark.offer                 16      MPSC_ARRAY  avgt    5    12.524 ±    2.845  ns/op
LatencyBenchmark.offer                 64  ARRAY_BLOCKING  avgt    5  3297.517 ± 2976.274  ns/op
LatencyBenchmark.offer                 64      MPSC_ARRAY  avgt    5    52.109 ±   50.199  ns/op
```
