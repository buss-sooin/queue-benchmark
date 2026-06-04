package com.apm.benchmark;

/**
 * agent의 QueueItem 복사본(벤치 전용).
 *
 * <p>agent 원본은 {@code record QueueItem(DataType type, Object data)}이다. 큐
 * 자료구조의 offer/drain 성능만 측정하므로 data 자리에 실제 proto 객체 대신 가벼운
 * Object를 넣는다. 큐에 담기는 객체의 내용은 offer/drain 속도에 영향을 주지 않는다.
 */
public record QueueItem(DataType type, Object data) {}
