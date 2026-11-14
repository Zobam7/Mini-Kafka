from typing import Any
from shared.models import OffsetError

class Segment:
  def __init__(self, base_offset: int):
    self._segment: list[Any] = []
    self._base_offset = base_offset
    
  def append(self, event):
    self._segment.append(event)
    return self._base_offset + len(self._segment) - 1
  
  def read(self, offset: int):
    if offset < self._base_offset:
      raise OffsetError(f"Offset is below the base")
    
    index = offset - self._base_offset
    if index > len(self._segment) - 1:
      raise OffsetError("Offset out of range")
    
    event = self._segment[index]
    return event