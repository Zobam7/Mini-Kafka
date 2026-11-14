from log import Log
from shared.models import NO_NEW_EVENT_SENTIEL
from pathlib import Path
import json
import os
from threading import RLock

_offsets_lock = RLock()

class Consumer:
  def __init__(self, log: Log, id: str | None = None, auto_commit = True, dir_path: Path = Path("logs")):
    self.id = id if id is not None else 'default'
    self.log = log
    self.auto_commit = auto_commit
    self._offset_path = dir_path / 'offset.json'
    self._tmp_offset_path = dir_path / 'offset.json.tmp'
    self._load()
    
    
  def read(self):
    if self.offset <= self.log.get_highest_offset():
      event = self.log.read_event(offset=self.offset)
      old_offset = self.offset
      self.offset += 1
      if self.auto_commit:
        self.commit()
      return event, old_offset
    else:
      return NO_NEW_EVENT_SENTIEL
    
  def commit(self):
    with _offsets_lock:
      self._save()
    
  def _save(self):
    try: 
      with open(self._offset_path, 'r') as file:
        data = json.load(file)
    except FileNotFoundError:
      data = {}
      
    data[self.id] = self.offset
    with open(self._tmp_offset_path, 'w') as file:
      json.dump(data, file, indent=4)
      file.flush()
      os.fsync(file.fileno())
    os.replace(self._tmp_offset_path, self._offset_path)

  
  def _load(self):
    with _offsets_lock:
      try:
        with open(self._offset_path, 'r') as file:
          data: dict = json.load(file)
          if not isinstance(data, dict):
            raise ValueError(f"Corrupted Offset file, Wrong Structure")
          
          offset = data.get(self.id, 0)
          if not isinstance(offset, int):
            raise TypeError('Offset is not an integer')
          
          if offset < 0:
            raise ValueError(f"Invalid offset, Negative offset not allowed")
          
          self.offset = int(offset)
      except FileNotFoundError:
        self.offset = 0
      except json.JSONDecodeError as e:
        raise ValueError(f"Corrupted Offset file: {e}")
      
  def restore_offset(self):
    self._load()