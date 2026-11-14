from segment import Segment
from shared.models import OffsetError, OffsetDeletedError, CorruptedSegmentError
from threading import RLock, Timer
from pathlib import Path
import struct

class Log:
  def __init__(self, max_segment_count: int = 3, max_segments: int = 10, should_run_timer=True, dir_path: Path = Path("logs")):
    if max_segment_count < 1:
      raise ValueError('max_segment_count must be at least 1')
    if max_segments < 1:
      raise ValueError('max_segments must be at least 1')
    self.max_segment_count = max_segment_count
    self.max_segments = max_segments
    self._lock = RLock()
    self.segments: list[Segment] = []
    self._log_dir = self.load_file(dir_path)
    self._active_segment = self.segments[-1]
    
    if should_run_timer:
      self._schedule_timer()

  def create_new_segment(self):
    with self._lock:
      base_offset = self._active_segment._base_offset
      next_offset = base_offset + len(self._active_segment._segment)
      path = self._log_dir / f'{next_offset:020}.log'
      seg = Segment(base_offset=next_offset)
      return seg, path


  def append(self, event):
    with self._lock:
      seg = None
      path = None
      if len(self._active_segment._segment) >= self.max_segment_count:
        seg, path = self.create_new_segment()
        
      self.append_event_to_file(event, path)
      
      if seg is not None:
        self.segments.append(seg)
        self._active_segment_path = path
        self._active_segment = self.segments[-1]

      
      offset = self._active_segment.append(event)
      return offset

  def read_event(self, offset: int):
    with self._lock:
      if offset < self.segments[0]._base_offset:
        raise OffsetDeletedError('The offset you are trying to read has been deleted')
      for segment in self.segments:
        if offset >= segment._base_offset and offset < segment._base_offset + len(segment._segment):
          event = segment.read(offset=offset)
          return event
        
      raise OffsetError("Offset out of range")
  
  def get_highest_offset(self):
    with self._lock:
      return self._active_segment._base_offset + len(self._active_segment._segment) - 1
    
  def enforce_retention(self):
    with self._lock:
      if len(self.segments) > self.max_segments:
        stop_pos = len(self.segments) - self.max_segments
          
        try:
          for _ in range(stop_pos):
            seg = self.segments[0]
            file = self._log_dir / f'{seg._base_offset:020}.log'
            file.unlink(missing_ok=True)
            del self.segments[0]
        except OSError:
          raise
        
  def _schedule_timer(self):
    self.timer = Timer(5.0, self.start_periodic_retention)
    self.timer.daemon = True
    self.timer.start()
    return self.timer
  
  def start_periodic_retention(self):
    self.enforce_retention()
    self._schedule_timer()
    
  def load_file(self, dir_path):
    with self._lock:
      path = Path("logs") if dir_path is None else dir_path
      if not path.is_dir():
        path.mkdir(exist_ok=True)

      has_segments = any(path.glob('*.log'))
      
      if not has_segments:
        self._active_segment_path = path / f'{0:020}.log'
        self.segments.append(Segment(base_offset=0))
        return path
        
      self.recover(path)
      offset = self.segments[-1]._base_offset
      self._active_segment_path = path / f'{offset:020}.log'
      return path
    
  def recover(self, path: Path):
    with self._lock:
      files = [f for f in path.iterdir() if f.is_file() and f.suffix == '.log']
      if not files:
        return
      sorted_files = sorted(files, key=lambda f: int(f.stem))
      last_file = sorted_files[- 1]
      for item in sorted_files:
        is_last_file = (item == last_file)
        self.read_event_from_file(item, is_last_file)

  def append_event_to_file(self, event: str, path: Path | None):
    with self._lock:
      event_bytes = event.encode('utf-8')
      event_length = len(event_bytes)
      event_header = struct.pack('>i', event_length)
      target_path = path if path is not None else self._active_segment_path
      
      with open(target_path, 'ab') as log_file:
        start = log_file.tell()
        try:
          log_file.write(event_header)
          log_file.write(event_bytes)
          log_file.flush()
        except OSError:
          log_file.truncate(start)
          raise

      
  def read_event_from_file(self, file: Path, is_last_file: bool):
    with self._lock:
      seg = Segment(base_offset=int(file.stem))
      position = 0
      with open(file, 'r+b') as log_file:
        log_content = log_file.read()
        while True:
          header_bytes = log_content[position: position + 4]
          if not header_bytes:
            break
          try:
            length_prefix = struct.unpack(">i", header_bytes)[0]
          except struct.error:
            if not is_last_file:
              raise CorruptedSegmentError(
                f"{file.name}: record at byte {position} expects 4 bytes, "
                f"found {len(header_bytes)}. Only the last segment can end in a partial "
                f"record, so this file is corrupted."
              )
            log_file.truncate(position)
            break
          start = position + 4
          event_bytes = log_content[start: start + length_prefix]
          if len(event_bytes) != length_prefix:
            if not is_last_file:
              raise CorruptedSegmentError(
                f"{file.name}: record at byte {position} expects {length_prefix} bytes, "
                f"found {len(event_bytes)}. Only the last segment can end in a partial "
                f"record, so this file is corrupted."
              )
            log_file.truncate(position)
            break
          event = event_bytes.decode('utf-8')
          seg.append(event)
          position = position + 4 + length_prefix
          
      self.segments.append(seg)