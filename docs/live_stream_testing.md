# Live Stream Testing

This document covers the current live-debug workflow for `mini_flink` and `bmproduce`.

The goal is to let you:

- generate live events
- inspect producer output in one terminal
- inspect streaming processor output in another terminal
- shape the stream to test key distribution and window behavior

## Current model

There are currently two live patterns:

1. direct producer to processor
2. independent feed server

### Direct producer to processor

This is the current end-to-end pairing that works with `bmrun run-live-streaming-graph`.

- `bmrun run-live-streaming-graph` listens for a TCP feed
- `bmproduce synthetic`, `bmproduce manual`, and `bmproduce replay` connect to it

### Independent feed server

This is the new source-first mode.

- `bmproduce serve tcp-clickstream` starts producing immediately
- it prints events locally even if no consumer is attached
- consumers can attach later and receive future events

Important limitation:
- `mini_flink` can now either listen for a feed or connect out to an existing TCP feed
- the first external attach mode is TCP client mode via `--connect`

## Terminal setup

Use `/home/mncubel/rs-bench` as the working directory.

### Terminal 1: processor

```bash
cd /home/mncubel/rs-bench
cargo run -p bmrun -- run-live-streaming-graph terminal_demo --listen 127.0.0.1:7001
```

Expected startup output:

```text
Starting live streaming graph
Graph: terminal_demo
Listen: 127.0.0.1:7001
Press Ctrl-C to stop the processor.
mini_flink live source listening on 127.0.0.1:7001
```

### Terminal 2: producer

```bash
cd /home/mncubel/rs-bench
cargo run -p bmproduce -- synthetic clickstream --connect 127.0.0.1:7001 --rate 4
```

The producer prints every emitted event.
The processor prints every closed window.

## Attaching to an existing feed

If the feed already exists independently, start the producer/feed first and then attach the processor later.

### Terminal 1: independent feed

```bash
cd /home/mncubel/rs-bench
cargo run -p bmproduce -- serve tcp-clickstream --listen 127.0.0.1:7001 --rate 4
```

### Terminal 2: attach processor later

```bash
cd /home/mncubel/rs-bench
cargo run -p bmrun -- run-live-streaming-graph terminal_demo --connect 127.0.0.1:7001
```

In this mode:

- the feed can start before the processor
- the processor connects out to the feed when you launch it
- the processor receives future events from the point of connection onward

## Producer commands

### Synthetic clickstream

Round-robin across devices:

```bash
cargo run -p bmproduce -- synthetic clickstream \
  --connect 127.0.0.1:7001 \
  --rate 4 \
  --pattern round-robin
```

Single key, good for watching one keyed aggregate build up:

```bash
cargo run -p bmproduce -- synthetic clickstream \
  --connect 127.0.0.1:7001 \
  --rate 8 \
  --pattern single-key \
  --sticky-device mobile
```

Burst mode, good for uneven traffic:

```bash
cargo run -p bmproduce -- synthetic clickstream \
  --connect 127.0.0.1:7001 \
  --rate 8 \
  --pattern burst \
  --burst-size 5
```

Finite stream:

```bash
cargo run -p bmproduce -- synthetic clickstream \
  --connect 127.0.0.1:7001 \
  --rate 4 \
  --count 20
```

Custom devices:

```bash
cargo run -p bmproduce -- synthetic clickstream \
  --connect 127.0.0.1:7001 \
  --rate 6 \
  --devices mobile,desktop \
  --pattern burst \
  --burst-size 3
```

Different event type:

```bash
cargo run -p bmproduce -- synthetic clickstream \
  --connect 127.0.0.1:7001 \
  --rate 4 \
  --event-type purchase
```

### Manual stdin

Use this when you want to paste raw `LiveInputEvent` JSON manually.

```bash
cargo run -p bmproduce -- manual stdin --connect 127.0.0.1:7001
```

Example input line:

```json
{"event_time_ms":0,"user_id":1,"session_id":1,"device_type":"mobile","event_type":"page_view","value":3,"key":null}
```

### Replay file

Replay a JSONL file containing one `LiveInputEvent` per line.

```bash
cargo run -p bmproduce -- replay file \
  --connect 127.0.0.1:7001 \
  --input events.jsonl \
  --rate 10
```

## Independent feed server

This starts a stream even if no consumer exists yet.

```bash
cargo run -p bmproduce -- serve tcp-clickstream \
  --listen 127.0.0.1:7001 \
  --rate 4 \
  --pattern round-robin
```

What it does:

- binds to `--listen`
- starts generating events immediately
- prints each emitted event as `served: ...`
- allows clients to attach later

When a client attaches, you will see:

```text
Client attached from 127.0.0.1:...
```

Current behavior for late consumers:

- they receive future events from the moment they connect
- they do not receive historical events that were already emitted

## Why the producer can fail

If you use `synthetic`, `manual`, or `replay` and see `Connection refused`, that means:

- those commands are TCP clients
- `run-live-streaming-graph` was not listening yet

In that mode, start the processor first.

## Reading the processor output

The `terminal_demo` graph in [apps/bmrun/src/streaming_graphs/terminal_demo.rs](/home/mncubel/rs-bench/apps/bmrun/src/streaming_graphs/terminal_demo.rs) currently uses:

- `key_by("device_type")`
- `window_tumbling_secs(1)`
- `aggregate_count_sum("event_count", "value", "value_sum")`
- `sink_stdout()`

So output like this:

```text
window=[4000-5000] key=mobile count=1 sum=5 avg=-
```

means:

- one 1-second tumbling window
- grouped by `device_type`
- `count` is the number of events in that window for that key
- `sum` is the sum of `value` for that window and key

## Good test scenarios

### Basic liveness

```bash
cargo run -p bmproduce -- synthetic clickstream --connect 127.0.0.1:7001 --rate 4
```

Use this to confirm the feed, routing, and stdout sink are all working.

### Aggregation on one key

```bash
cargo run -p bmproduce -- synthetic clickstream \
  --connect 127.0.0.1:7001 \
  --rate 8 \
  --pattern single-key \
  --sticky-device mobile
```

Use this to see multiple events accumulate into the same keyed window.

### Key skew / burstiness

```bash
cargo run -p bmproduce -- synthetic clickstream \
  --connect 127.0.0.1:7001 \
  --rate 8 \
  --pattern burst \
  --burst-size 5
```

Use this to simulate uneven traffic and watch how window outputs change.

### Manual edge-case injection

```bash
cargo run -p bmproduce -- manual stdin --connect 127.0.0.1:7001
```

Use this when you want to feed exact events by hand.

## Next step

The next planned improvement is to let `mini_flink` connect out to independent feeds such as:

- TCP feed servers
- WebSocket feeds
- later possibly SSE or broker-backed sources

That will enable true “attach to an existing live feed at any time” behavior from the processor side.
