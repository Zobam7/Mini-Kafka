class OffsetError(IndexError):
  pass

class OffsetDeletedError(IndexError):
  pass

class CorruptedSegmentError(Exception):
  pass

NO_NEW_EVENT_SENTIEL = object()