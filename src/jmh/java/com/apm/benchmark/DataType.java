package com.apm.benchmark;

/**
 * agent의 DataType 복사본(벤치 전용).
 *
 * <p>큐 자료구조의 경합 성능만 측정하므로 enum 값의 실제 의미는 중요하지 않다.
 * QueueItem을 구성할 타입 자리만 채운다.
 */
public enum DataType {
    METRICS,
    SPAN,
    LOG
}
