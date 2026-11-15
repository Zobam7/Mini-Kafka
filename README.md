# Mini Kafka (Python / asyncio)

A small event streaming system, modeled on Kafka: an append-only log split into
segment files, consumers that track their own position, and a TCP server that
speaks a simple length-prefixed text protocol.

Built after a multithreaded downloader, an async reverse proxy, a Redis-backed job queue, and a Redis clone.

## What it does

- **Append-only log.** Producers append events. Each event gets an **offset**, a
  number that never changes. Reading never removes anything.
- **Segments.** The log is split into segments. Each segment is its own file,
  named after the offset of its first event (`00000000000000000003.log`).
- **Independent consumers.** Each consumer has an id and its own saved position,
  so several services can read the same log at their own pace.
- **Durability.** Events are written to disk before they are acknowledged, and
  the log recovers after a crash.
- **Retention.** Old segments are deleted, whole files at a time, once the log
  has more than a set number of segments.
- **At-least-once delivery.** A consumer saves its position only when the client
  sends `COMMIT`. If a client crashes before committing, it gets the same event
  again. An event can be delivered twice. It is never skipped.

## Project layout

```
mini-kafka/
├── segment.py        # One segment in memory: events + base offset
├── log.py            # The log: rollover, files, recovery, retention
├── consumer.py       # A consumer: its position, saved to offset.json
├── server.py         # asyncio TCP server and protocol
├── shared/
│   └── models.py     # Exceptions and the "no new event" sentinel
└── kafka_test.py     # Tests (pytest)
```

## Running it

```bash
python server.py
```

The server listens on `localhost:8003` and stores its files in `./logs`.
`startup()` uses small demo values (3 events per segment, 4 segments kept), so
change them for real use.

Run the tests with:

```bash
python -m pytest
```

## Protocol

Every command is one line followed by exactly that many bytes:

```
<COMMAND> <n>\n
<n bytes>
```

The count is in **bytes**, not characters. The server reads the body by count and
never looks inside it, so an event can contain newlines or any other character.

| Command      | Body            | Reply                                                    |
| ------------ | --------------- | -------------------------------------------------------- |
| `APPEND <n>` | the event       | `OK <offset>`                                            |
| `READ <n>`   | the consumer id | `EVENT <offset> <n>` followed by `<n>` bytes, or `EMPTY` |
| `COMMIT <n>` | the consumer id | `OK`                                                     |

Any failure replies `ERROR <CODE> <message>`.

`EMPTY` is a normal answer, not an error: the consumer has caught up.

| Code              | Meaning                                          | Connection |
| ----------------- | ------------------------------------------------ | ---------- |
| `WRONG_TYPE`      | the byte count is missing or not a number        | closed     |
| `TOO_LARGE`       | the count is over 1 MB                           | closed     |
| `BAD_REQUEST`     | the count is zero or negative                    | closed     |
| `UNKNOWN_COMMAND` | the command does not exist                       | closed     |
| `INCOMPLETE_BODY` | the client closed before sending all the bytes   | closed     |
| `READ_TIMEOUT`    | the body took longer than 5 seconds              | closed     |
| `ID_IN_USE`       | another connection is using this consumer id     | closed     |
| `BAD_REQUEST`     | the body is not valid UTF-8                      | stays open |
| `OFFSET_DELETED`  | the consumer's position was removed by retention | stays open |
| `INTERNAL`        | any other server error                           | stays open |

The connection is closed when the server can no longer tell where the next
command starts. It stays open when the whole body was read and the stream is
still in sync.

### Example client

```python
import socket

sock = socket.create_connection(("localhost", 8003))
reader = sock.makefile("rb")

def send(command, text):
    body = text.encode("utf-8")
    sock.sendall(f"{command} {len(body)}\n".encode() + body)

def reply():
    header = reader.readline().decode().split()
    if header[0] == "EVENT":
        offset, size = int(header[1]), int(header[2])
        return ("EVENT", offset, reader.read(size).decode("utf-8"))
    return tuple(header)

send("APPEND", "hello\nworld");  print(reply())   # ('OK', '0')
send("READ", "email-service");   print(reply())   # ('EVENT', 0, 'hello\nworld')
send("COMMIT", "email-service"); print(reply())   # ('OK',)
send("READ", "email-service");   print(reply())   # ('EMPTY',)
```

Replies are read the same way in both directions: a header line carries the
byte count, then exactly that many bytes follow.

## Design decisions

**Segment files are named by their first offset.** Retention deletes whole files,
nothing is renumbered, and the log rebuilds itself on restart from the file
names. Each segment looks up an event with `offset - base_offset`.

**Records are length-prefixed, not separated by newlines.** Each record is a
4-byte length followed by the event bytes, the same idea as Kafka's records and
HTTP's `Content-Length`. Any event can be stored, and a crash during a write is
easy to detect: the file ends before the length says it should.

**Disk before memory.** `append` writes the record to the file first and updates
the in-memory log only if the write worked. A failed write is truncated back, so
memory, disk and the producer's reply stay in agreement. A new segment is also
added only after its first write succeeds.

**Crash recovery.** On startup the log reads every segment file. A half-written
record at the end of the **last** file is expected after a crash, so it is cut
off. A damaged record in any **older** file cannot come from a crash, so it
raises `CorruptedSegmentError` and leaves the file untouched.

**Positions are saved on `COMMIT`, not on `READ`.** `read()` moves the consumer
forward in memory. `commit()` saves the position. Committing twice saves the
same number twice, so it is safe to repeat. The server creates its consumers
with auto-commit off and throws them away when a connection ends, so the next
connection reloads the last committed position.

**The offsets file is replaced, not edited.** All consumers' positions live in
one `offset.json`. A commit reads it, changes one entry, writes a temporary file,
`fsync`s it, and swaps it in with `os.replace`. A crash leaves either the old
file or the new one, never half of each.

**One connection per consumer id.** Two clients sharing an id would share one
position, and one could commit past events the other never handled. The server
refuses the second connection with `ID_IN_USE`, and releases the id in a
`finally` block when the connection ends for any reason.

**"No new event" is a value, not an error.** In Python it is a unique `object()`
sentinel that no event can equal, compared with `is`. On the wire it is the
reply word `EMPTY`.

## Concurrency

The server is single-threaded `asyncio`, so commands never interleave in the
middle of a method. Threads exist in two places, and both are guarded:
the retention timer thread, and any code that uses `Log` or `Consumer` directly
from threads. `Log` has one `RLock`, and consumers share one module-level lock
for the offsets file.

## Known limitations

- **One process per log directory.** The locks only work inside one process. Run
  a single server per directory. Two processes writing the same `offset.json`
  or segment files are not safe.
- **Claims are a module-level dictionary** in `server.py`. Two servers in one
  process would share them. They should be created in `startup()` and injected.
- **Retention counts segments only.** There is no size-based or time-based
  retention.
- **No offset reset policy.** A consumer whose saved position was deleted gets
  `OFFSET_DELETED` on every `READ`. A saved position beyond the end of the log
  returns `EMPTY` until the log catches up.
- **A single log.** No topics, partitions, replication, authentication or TLS.
- **Corrupted offsets file.** It raises `ValueError`, or `TypeError` when an
  offset is not an integer. These should be one exception type.
- **Not covered by automated tests:** the retention timer, the 5-second body
  timeout, non-ASCII events over the wire, and releasing consumer ids after bad
  bytes or a disconnect. These were checked by hand.
