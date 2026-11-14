import asyncio
from functools import partial
from log import Log
from consumer import Consumer
from shared.models import NO_NEW_EVENT_SENTIEL, OffsetDeletedError

MAX_SIZE = 1024 * 1024
ALLOWED_COMMANDS = ['APPEND', 'READ', 'COMMIT']

store = {}

def check_and_claim(writer: asyncio.StreamWriter, consumer_id, log: Log):
  peername = writer.get_extra_info('peername')
  
  if consumer_id not in store:
    consumer_instance = Consumer(log, id=consumer_id, auto_commit=False, dir_path=log._log_dir)
    store[consumer_id] = {
      'peername': peername,
      'consumer': consumer_instance
    }
    return True
  
  record = store.get(consumer_id)
  if record is None:
    return False
  
  if record['peername'] == peername:
    return True
  return False

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, log: Log):
  current_consumer_ids = set()
  
  try:
    while True:
      line = await reader.readline()
      if not line:
        break
      
      parts = line.decode().split()
      try:
        size = int(parts[1])
      except (ValueError, IndexError):
        writer.write(b'ERROR WRONG_TYPE Number of bytes must be an integer\n')
        await writer.drain()
        break
        
      if size > MAX_SIZE:
        writer.write(b'ERROR TOO_LARGE Max number of bytes allowed is 1MB\n')
        await writer.drain()
        break
      
      if size <= 0: 
        writer.write(b'ERROR BAD_REQUEST Bytes can not be less than or equal to zero\n')
        await writer.drain()
        break
        
      command = parts[0]
      if command not in ALLOWED_COMMANDS:
        writer.write(b'ERROR UNKNOWN_COMMAND Command is unknown\n')
        await writer.drain()
        break
      
      try:
        # body = await reader.readexactly(size)
        body = await asyncio.wait_for(reader.readexactly(size), timeout=5.0)
      except asyncio.TimeoutError:
        writer.write(b'ERROR READ_TIMEOUT Read took too long to complete\n')
        await writer.drain()
        break
      except asyncio.IncompleteReadError:
        writer.write(b'ERROR INCOMPLETE_BODY Body is not complete\n')
        await writer.drain()
        break
        

      try:
        payload = body.decode('utf-8')
        if command == 'APPEND':
          offset = log.append(payload)
          writer.write(f"OK {offset}\n".encode())
        elif command == 'READ':
          has_access = check_and_claim(writer, payload, log)
          if not has_access:
            writer.write(b'ERROR ID_IN_USE consumer id is already connected\n')
            await writer.drain()
            break
          
          current_consumer_ids.add(payload)
          consumer = store[payload]['consumer']
          
          data = consumer.read()
          if NO_NEW_EVENT_SENTIEL is data:
            writer.write(f"EMPTY\n".encode())
          else:
            event, offset = data
            encoded_event = event.encode('utf-8')
            writer.write(f"EVENT {offset} {len(encoded_event)}\n".encode() + encoded_event)
            
        elif command == 'COMMIT':
          has_access = check_and_claim(writer, payload, log)
          if not has_access:
            writer.write(b'ERROR ID_IN_USE consumer id is already connected\n')
            await writer.drain()
            break
          
          current_consumer_ids.add(payload)
          consumer = store[payload]['consumer']
          
          consumer.commit()
          writer.write(f"OK\n".encode())
        else:
          writer.write(b'ERROR UNKNOWN_COMMAND Command is unknown\n')
      except OffsetDeletedError:
        writer.write(b'ERROR OFFSET_DELETED The offset you are trying to read has been deleted\n')
      except UnicodeDecodeError:
        writer.write(b'ERROR BAD_REQUEST Invalid Bytes recevied\n')
      except Exception as e:
        writer.write(("ERROR INTERNAL " + str(e) + "\n").encode('utf-8'))
      

      await writer.drain()
  finally:
    for id in current_consumer_ids:
      store.pop(id, None)
    writer.close()
  await writer.wait_closed()


async def startup(log=None, host='localhost', port=8003):
  if log is None:
    log = Log(max_segment_count=3, max_segments=4)
    
  handler = partial(handle_client, log = log)
  server = await asyncio.start_server(handler, host, port)
  
  return server

async def main():
  server = await startup()
  port = server.sockets[0].getsockname()[1]
  print(f"Server listening on localhost:{port}")
  async with server:
    try:
      await server.serve_forever()
    except (asyncio.CancelledError, KeyboardInterrupt):
      tasks = asyncio.all_tasks() - {asyncio.current_task()}
      for task in tasks:
        task.cancel()
      await asyncio.gather(*tasks, return_exceptions=True)
      print("shutdown signal receieved")
      return


if __name__ == "__main__":
  asyncio.run(main())