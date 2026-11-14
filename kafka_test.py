import pytest
from segment import Segment
from shared.models import OffsetError, NO_NEW_EVENT_SENTIEL, OffsetDeletedError, CorruptedSegmentError
from log import Log
from consumer import Consumer
import struct
import json
from concurrent.futures import ThreadPoolExecutor
import asyncio
from server import startup

@pytest.fixture()
def populated_segment():
  segment = Segment(base_offset=3)
  
  offsets = {
    "offset_1": segment.append('Event 3'),
    "offset_2": segment.append('Event 4'),
    "offset_3": segment.append('Event 5')
  }
  
  return segment, offsets

@pytest.fixture()
def populated_logs(tmp_path):
  log = Log(max_segment_count=3, should_run_timer=False, dir_path=tmp_path / 'logs')
  log.append("Event 1")
  log.append("Event 2")
  log.append("Event 3")
  log.append("Event 4")
  log.append("Event 5")
  log.append("Event 6")
  log.append("Event 7")
  log.append("Event 8")
  
  return log

def test_segment_append_new_events_returns_correct_offsets(populated_segment):
  segment, offsets = populated_segment
  assert offsets["offset_1"] == 3
  assert offsets["offset_2"] == 4
  assert offsets["offset_3"] == 5
  
def test_read_offset_does_not_mutate_the_segment(populated_segment):
  segment, offsets = populated_segment
  
  event = segment.read(4)
  assert event == 'Event 4'
  assert segment._segment == ['Event 3', 'Event 4', 'Event 5']
  
def test_reading_offset_below_base_raises(populated_segment):
  segment, offsets = populated_segment
  
  with pytest.raises(OffsetError, match="Offset is below the base"):
    segment.read(2)
    
def test_reading_offset_with_negative_value(populated_segment):
  segment, offsets = populated_segment
  
  with pytest.raises(OffsetError, match="Offset is below the base"):
    segment.read(-1)
    
def test_reading_offset_larger_than_the_segment(populated_segment):
  segment, offsets = populated_segment
  
  with pytest.raises(OffsetError, match="Offset out of range"):
    segment.read(6)
    

def test_log_with_max_segment_count_creates_correct_number_of_segments(tmp_path):
  log = Log(max_segment_count=3, should_run_timer=False, dir_path=tmp_path / 'logs')
  log.append("Event 1")
  log.append("Event 2")
  log.append("Event 3")
  log.append("Event 4")
  
  assert len(log.segments) == 2
  assert log.segments[-1]._segment == ['Event 4']
  
  
def test_read_event_from_log_with_offset(populated_logs):
  log = populated_logs
  
  assert len(log.segments) == 3
  assert log.read_event(2) == 'Event 3'
  assert log.read_event(5) == 'Event 6'
  assert log.read_event(7) == 'Event 8'
  
def test_read_event_from_log_with_offset_larger_than_log(populated_logs):
  log = populated_logs
  
  with pytest.raises(OffsetError, match="Offset out of range"):
    log.read_event(10)
    

def test_log_read_returns_correct_event_after_deleting_a_segment(populated_logs):
  log = populated_logs
  
  log.segments.pop(0)
  assert log.read_event(5) == 'Event 6'
  
def test_get_highest_offset_in_logs(populated_logs):
  log = populated_logs
  assert log.get_highest_offset() == 7
  
def test_consumer_read_until_highest_offset_and_returns(populated_logs, tmp_path):
  log = populated_logs
  consumer = Consumer(log, dir_path=tmp_path / 'logs')
  assert ('Event 1', 0) == consumer.read()
  assert ('Event 2', 1) == consumer.read()
  assert ('Event 3', 2) == consumer.read()
  assert ('Event 4', 3) == consumer.read()
  assert ('Event 5', 4) == consumer.read()
  assert ('Event 6', 5) == consumer.read()
  assert ('Event 7', 6) == consumer.read()
  assert ('Event 8', 7) == consumer.read()
  assert NO_NEW_EVENT_SENTIEL is consumer.read()
  
def test_multiple_consumers_do_not_change_another_consumer_offsets(populated_logs, tmp_path):
  log = populated_logs
  consumer1 = Consumer(log, dir_path=tmp_path / 'logs')
  consumer2 = Consumer(log, dir_path=tmp_path / 'logs')
  
  assert ('Event 1', 0) == consumer1.read()
  assert ('Event 2', 1) == consumer1.read()
  assert ('Event 3', 2) == consumer1.read()
  assert ('Event 1', 0) == consumer2.read()
  
def test_read_event_from_a_deleted_offset_raises_OffsetDeletedError(populated_logs, tmp_path):
  log = populated_logs
  consumer = Consumer(log, dir_path=tmp_path / 'logs')
  log.segments.pop(0)
  with pytest.raises(OffsetDeletedError, match='The offset you are trying to read has been deleted'):
    consumer.read()
    
def test_retention_with_max_segments_in_a_log(tmp_path):
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
  log.append("Event 1")
  log.append("Event 2")
  log.append("Event 3")
  
  log.append("Event 4")
  log.append("Event 5")
  log.append("Event 6")
  
  log.append("Event 7")
  log.append("Event 8")
  log.append("Event 9")
  
  log.append("Event 10")
  log.append("Event 11")
  log.append("Event 12")
  
  log.append("Event 13")
  
  log.enforce_retention()
  
  assert len(log.segments) == 4
  
def test_append_events_to_file(tmp_path):
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
  offset = log.append("Event 1")
  segment_file = f'{offset:020}.log'
  test_file = tmp_path / 'logs' / segment_file
  binary_content = test_file.read_bytes()
  length_prefix = struct.unpack(">I", binary_content[:4])[0]
  actual_text = binary_content[4:].decode("utf-8")

  assert length_prefix == len("Event 1")
  assert actual_text == "Event 1"
  
def test_read_events_from_file(tmp_path):
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / "logs")
  log.append("Event 1")
  log.append("Event 2\n")
  log.append("Event 3")
  log2 = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / "logs")
  consumer = Consumer(log2, dir_path=tmp_path / 'logs')
  assert ('Event 1', 0) == consumer.read()
  assert ('Event 2\n', 1) == consumer.read()
  assert ('Event 3', 2) == consumer.read()
  
def test_partial_events_in_file_is_truncated(tmp_path):
  logs_dir = tmp_path / 'logs'
  logs_dir.mkdir()

  good = struct.pack('>i', 7) + b'Event 1'     
  broken = struct.pack('>i', 7) + b'Eve' 
  log_file = logs_dir / f'{0:020}.log'

  log_file.write_bytes(good + broken)
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=logs_dir)
  consumer = Consumer(log, dir_path=tmp_path / 'logs')
  assert ('Event 1', 0) == consumer.read()
  assert NO_NEW_EVENT_SENTIEL is consumer.read()
  assert log_file.read_bytes() == good

def test_partial_length_field_in_file_is_truncated(tmp_path):
  logs_dir = tmp_path / 'logs'
  logs_dir.mkdir()

  good = struct.pack('>i', 7) + b'Event 1'     
  broken = struct.pack('>i', 7)[:2]
  log_file = logs_dir / f'{0:020}.log'

  log_file.write_bytes(good + broken)
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=logs_dir)
  consumer = Consumer(log, dir_path=tmp_path / 'logs')
  assert ('Event 1', 0) == consumer.read()
  assert NO_NEW_EVENT_SENTIEL is consumer.read()
  assert log_file.read_bytes() == good

def test_append_new_event_after_recovery(tmp_path):
  logs_dir = tmp_path / 'logs'
  logs_dir.mkdir()

  first = struct.pack('>i', 7) + b'Event 1'     
  second = struct.pack('>i', 7) + b'Event 2'     
  broken = struct.pack('>i', 7) + b'Eve' 
  log_file = logs_dir / f'{0:020}.log'

  log_file.write_bytes(first + second + broken)
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=logs_dir)
  log.append('Event 3')
  consumer = Consumer(log, dir_path=tmp_path / 'logs')
  assert ('Event 1', 0) == consumer.read()
  assert ('Event 2', 1) == consumer.read()
  assert ('Event 3', 2) == consumer.read()
  
  third = struct.pack('>i', 7) + b'Event 3'  # to confirm Event 3 is appended to the end of the disk after truncating
  assert log_file.read_bytes() == first + second + third

def test_older_segment_with_damaged_record_raises(tmp_path):
  logs_dir = tmp_path / 'logs'
  logs_dir.mkdir()

  first = struct.pack('>i', 7) + b'Event 1'     
  second = struct.pack('>i', 7) + b'Event 2'     
  broken = struct.pack('>i', 7)[:2] 
  third = struct.pack('>i', 7) + b'Event 4' 
  fourth = struct.pack('>i', 7) + b'Event 5' 
  log_file = logs_dir / f'{0:020}.log'
  log_file_2 = logs_dir / f'{3:020}.log'

  log_file.write_bytes(first + second + broken)
  log_file_2.write_bytes(third + fourth)
  with pytest.raises(CorruptedSegmentError, match='is corrupted'):
    Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=logs_dir)
  assert log_file.read_bytes() == first + second + broken
  
def test_older_segment_starts_with_damaged_record_raises(tmp_path):
  logs_dir = tmp_path / 'logs'
  logs_dir.mkdir()

  broken = struct.pack('>i', 7)[:2] 
  first = struct.pack('>i', 7) + b'Event 1'     
  second = struct.pack('>i', 7) + b'Event 2'     
  third = struct.pack('>i', 7) + b'Event 4' 
  fourth = struct.pack('>i', 7) + b'Event 5' 
  log_file = logs_dir / f'{0:020}.log'
  log_file_2 = logs_dir / f'{3:020}.log'

  log_file.write_bytes(broken + first + second)
  log_file_2.write_bytes(third + fourth)
  with pytest.raises(CorruptedSegmentError, match='is corrupted'):
    Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=logs_dir)
  assert log_file.read_bytes() == broken + first + second
  
def test_enforce_retention_deletes_from_disk(tmp_path):
  logs_dir = tmp_path / 'logs'
  logs_dir.mkdir()
  seg_1 = logs_dir / f'{0:020}.log'
  seg_2 = logs_dir / f'{3:020}.log'
  seg_3 = logs_dir / f'{6:020}.log'
  seg_4 = logs_dir / f'{9:020}.log'
  seg_5 = logs_dir / f'{12:020}.log'
  
  first = struct.pack('>i', 7) + b'Event 1'     
  second = struct.pack('>i', 7) + b'Event 2'   
  third = struct.pack('>i', 7) + b'Event 3' 
  fourth = struct.pack('>i', 7) + b'Event 4' 
  event_5 = struct.pack('>i', 7) + b'Event 5' 
  event_6 = struct.pack('>i', 7) + b'Event 6' 
  event_7 = struct.pack('>i', 7) + b'Event 7' 
  event_8 = struct.pack('>i', 7) + b'Event 8' 
  event_9 = struct.pack('>i', 7) + b'Event 9' 
  event_10 = struct.pack('>i', 8) + b'Event 10' 
  event_11 = struct.pack('>i', 8) + b'Event 11' 
  event_12 = struct.pack('>i', 8) + b'Event 12' 
  event_13 = struct.pack('>i', 8) + b'Event 13' 
  
  seg_1.write_bytes(first + second + third)
  seg_2.write_bytes(fourth + event_5 + event_6)
  seg_3.write_bytes(event_7 + event_8 + event_9)
  seg_4.write_bytes(event_10 + event_11 + event_12)
  seg_5.write_bytes(event_13)
  
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=logs_dir)
  log.enforce_retention()
  file_count = len(list(logs_dir.iterdir()))
  assert file_count == 4
  assert not seg_1.exists()
  assert seg_2.exists()
  assert seg_3.exists()
  assert seg_4.exists()
  assert seg_5.exists()
  
def test_deleted_segments_do_not_comeback_after_restart(tmp_path):
  logs_dir = tmp_path / 'logs'
  logs_dir.mkdir()
  seg_1 = logs_dir / f'{0:020}.log'
  seg_2 = logs_dir / f'{3:020}.log'
  seg_3 = logs_dir / f'{6:020}.log'
  seg_4 = logs_dir / f'{9:020}.log'
  seg_5 = logs_dir / f'{12:020}.log'
  
  first = struct.pack('>i', 7) + b'Event 1'     
  second = struct.pack('>i', 7) + b'Event 2'   
  third = struct.pack('>i', 7) + b'Event 3' 
  fourth = struct.pack('>i', 7) + b'Event 4' 
  event_5 = struct.pack('>i', 7) + b'Event 5' 
  event_6 = struct.pack('>i', 7) + b'Event 6' 
  event_7 = struct.pack('>i', 7) + b'Event 7' 
  event_8 = struct.pack('>i', 7) + b'Event 8' 
  event_9 = struct.pack('>i', 7) + b'Event 9' 
  event_10 = struct.pack('>i', 8) + b'Event 10' 
  event_11 = struct.pack('>i', 8) + b'Event 11' 
  event_12 = struct.pack('>i', 8) + b'Event 12' 
  event_13 = struct.pack('>i', 8) + b'Event 13' 
  
  seg_1.write_bytes(first + second + third)
  seg_2.write_bytes(fourth + event_5 + event_6)
  seg_3.write_bytes(event_7 + event_8 + event_9)
  seg_4.write_bytes(event_10 + event_11 + event_12)
  seg_5.write_bytes(event_13)
  
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=logs_dir)
  log.enforce_retention()
  
  log2 = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=logs_dir)
  with pytest.raises(OffsetDeletedError, match='has been deleted'):
    log2.read_event(0)
  
  assert log2.read_event(3) == 'Event 4'
  
def test_failed_write_does_not_advance_the_log(tmp_path, monkeypatch):
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
  log.append("Event 1")
  log.append("Event 2")
  log.append("Event 3")

  def broken_write(*args, **kwargs):
    raise OSError("disk full")

  monkeypatch.setattr(log, "append_event_to_file", broken_write)
  # log.append("Event 4")
  with pytest.raises(OSError, match="disk full"):
    log.append("Event 4")
    
  assert log.get_highest_offset() == 2
  
  
def test_max_segment_count_must_be_atleast_1(tmp_path):
  with pytest.raises(ValueError, match="max_segment_count must be at least 1"):
    Log(max_segment_count=0, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
    
def test_max_segments_must_be_atleast_1(tmp_path):
  with pytest.raises(ValueError, match="max_segments must be at least 1"):
    Log(max_segment_count=3, max_segments=0, should_run_timer=False, dir_path=tmp_path / 'logs')
    
    
def test_mulitple_logs(tmp_path):
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
  log.append("Event 1")
  log.append("Event 2")
  log.append("Event 3")
  log.append("Event 4")
  
  log2 = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
  assert log2.read_event(3) == 'Event 4'
  assert len(log2.segments) == 2

def test_consumer_offset_is_saved(tmp_path):
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
  log.append("Event 1")
  log.append("Event 2")
  log.append("Event 3")
  log.append("Event 4")
  
  consumer_1 = Consumer(log, id='email-service', auto_commit=False, dir_path=tmp_path / 'logs')
  assert ('Event 1', 0) == consumer_1.read()
  consumer_1.commit()
  assert ('Event 2', 1) == consumer_1.read()
  consumer_1.commit()
  
  consumer_2 = Consumer(log, id='email-service', auto_commit=False, dir_path=tmp_path / 'logs')
  assert ('Event 3', 2) == consumer_2.read()
  consumer_2.commit()
  
  
  consumer_3 = Consumer(log, id='analytics', auto_commit=False, dir_path=tmp_path / 'logs')
  assert ('Event 1', 0) == consumer_3.read()
  consumer_3.commit()
  
  consumer_2 = Consumer(log, id='email-service', auto_commit=False, dir_path=tmp_path / 'logs')
  assert ('Event 4', 3) == consumer_2.read()
  
def test_multiple_commit_is_safe(tmp_path):
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
  log.append("Event 1")
  log.append("Event 2")
  log.append("Event 3")
  log.append("Event 4")
  
  consumer_1 = Consumer(log, id='email-service', auto_commit=False, dir_path=tmp_path / 'logs')
  assert ('Event 1', 0) == consumer_1.read()
  consumer_1.commit()
  consumer_1.commit()
  assert ('Event 2', 1) == consumer_1.read()
  
def test_offset_file_raises_valueError_for_wrong_structure(tmp_path):
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
  log.append("Event 1")
  log.append("Event 2")
  log.append("Event 3")
  log.append("Event 4")
  
  offset_dir = tmp_path / 'logs'
  offset_dir.parent.mkdir(parents=True, exist_ok=True)
  offset_path = offset_dir / 'offset.json'
  data = [{"email-service": 2}]
  offset_path.write_text(json.dumps(data, indent=4))
  with pytest.raises(ValueError, match='Corrupted Offset file, Wrong Structure'):
    Consumer(log, id='email-service', auto_commit=False, dir_path=tmp_path / 'logs')
    
def test_offset_file_raises_value_error_for_broken_json(tmp_path):
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
  log.append("Event 1")
  log.append("Event 2")
  log.append("Event 3")
  log.append("Event 4")
  
  offset_dir = tmp_path / 'logs'
  offset_dir.parent.mkdir(parents=True, exist_ok=True)
  offset_path = offset_dir / 'offset.json'
  offset_path.write_text("[{'email-service': 2")
  with pytest.raises(ValueError, match='Corrupted Offset file'):
    Consumer(log, id='email-service', auto_commit=False, dir_path=tmp_path / 'logs')
    
def test_offset_file_raises_type_error_for_string_offsets(tmp_path):
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
  log.append("Event 1")
  log.append("Event 2")
  log.append("Event 3")
  log.append("Event 4")
  
  offset_dir = tmp_path / 'logs'
  offset_dir.parent.mkdir(parents=True, exist_ok=True)
  offset_path = offset_dir / 'offset.json'
  data = {"email-service": '2'}
  offset_path.write_text(json.dumps(data, indent=4))
  with pytest.raises(TypeError, match='Offset is not an integer'):
    Consumer(log, id='email-service', auto_commit=False, dir_path=tmp_path / 'logs')
    
def test_concurrent_consumers_commits_do_not_corrupt_offset_file(tmp_path):
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
  log.append("Event 1")
  log.append("Event 2")
  log.append("Event 3")
  log.append("Event 4")
  
  consumer_1 = Consumer(log, id='email-service', auto_commit=False, dir_path=tmp_path / 'logs')
  consumer_2 = Consumer(log, id='analytics', auto_commit=False, dir_path=tmp_path / 'logs')
  
  
  with ThreadPoolExecutor(max_workers=2) as executor:
    executor.submit(lambda: (consumer_1.read(), consumer_1.commit()))
    executor.submit(lambda: (consumer_2.read(), consumer_2.commit()))

  offset_dir = tmp_path / 'logs' / 'offset.json'
  offset_data = json.loads(offset_dir.read_text())
  assert {'email-service': 1, 'analytics': 1} == offset_data


def test_crashed_client_before_commit_return_same_event_on_restart(tmp_path):
  log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
  log.append("Event 1")
  log.append("Event 2")
  log.append("Event 3")
  log.append("Event 4")
  
  consumer = Consumer(log, id='email-service', auto_commit=False, dir_path=tmp_path / 'logs')
  consumer.read()
  # client crashes after read before running commit and restarts
  consumer.restore_offset()
  assert ("Event 1", 0) == consumer.read()
  
  consumer.commit()
  consumer.restore_offset()
  assert ('Event 2', 1) == consumer.read()
  
async def read_reply(reader):
  header = (await reader.readline()).decode().split()
  if header[0] == 'EVENT':
    offset, size = int(header[1]), int(header[2])
    body = await reader.readexactly(size)
    return ('EVENT', offset, body.decode('utf-8'))
  elif header[0] == 'ERROR':
    return ('ERROR', header[1])
  return tuple(header)


def test_server(tmp_path):
  replies = []
  async def scenario():
    log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
    host = '127.0.0.1'
    server = await startup(log, host, 0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection(host, port)
    
    event = "a\nb".encode('utf-8')
    writer.write(f"APPEND {len(event)}\n".encode() + event)
    # event_2 = "bc\nc".encode('utf-8')
    # writer.write(f"APPEND {len(event_2)}\n".encode() + event_2)
    id = 'email-service'.encode('utf-8')
    writer.write(f"READ {len(id)}\n".encode() + id)
    writer.write(f"COMMIT {len(id)}\n".encode() + id)
    writer.write(f"READ {len(id)}\n".encode() + id)
    id_2 = 'analytics'.encode('utf-8')
    writer.write(f"READ {len(id_2)}\n".encode() + id_2)
    
    for _ in range(5):
      reply = await read_reply(reader)
      replies.append(reply)
    
    writer.close()
    server.close()
    await server.wait_closed()
    return replies

  assert asyncio.run(scenario()) == [('OK', '0'), ('EVENT', 0, 'a\nb'), ('OK',), ('EMPTY',), ('EVENT', 0, 'a\nb')]
  
def test_client_sends_invalid_bytes(tmp_path):
  replies = []
  async def scenario():
    log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
    host = '127.0.0.1'
    server = await startup(log, host, 0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection(host, port)
    
    broken = struct.pack('>i', 7)[:2]
    writer.write(f"APPEND 7\n".encode() + broken)
    writer.write_eof() 
    
    reply = await read_reply(reader)
    writer.close()
    server.close()
    await server.wait_closed()

    return reply
  
  assert asyncio.run(scenario()) == ('ERROR', 'INCOMPLETE_BODY',)
  
  async def scenario_2():
    log = Log(max_segment_count=3, max_segments=4, should_run_timer=False, dir_path=tmp_path / 'logs')
    host = '127.0.0.1'
    server = await startup(log, host, 0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection(host, port)
    
    event = "New Event\nb".encode('utf-8')
    writer.write(f"APPEND {len(event)}\n".encode() + event)
    id = 'email-service'.encode('utf-8')
    writer.write(f"READ {len(id)}\n".encode() + id)
    writer.write(f"COMMIT {len(id)}\n".encode() + id)
    
    for _ in range(3):
      reply = await read_reply(reader)
      replies.append(reply)
    
    writer.close()
    server.close()
    await server.wait_closed()
    return replies
  
  assert asyncio.run(scenario_2()) == [('OK', '0'), ('EVENT', 0, 'New Event\nb'), ('OK',)]