import multiprocessing
import time
import struct
from multiprocessing import shared_memory, Lock
import datetime

TOKEN_SIZE = 64  # Max size for the token string
PACK_FORMAT = f'{TOKEN_SIZE}s d'  # Format: Token as a fixed-size byte string + a double for expiration time

def generate_token():
    """Simulate token generation."""
    new_token = f"token_{int(time.time())}".encode('utf-8')
    new_token = new_token.ljust(TOKEN_SIZE, b'\0')  # Ensure token has a fixed size
    expiration_time = (datetime.datetime.now() + datetime.timedelta(hours=2)).timestamp()
    print(f"Token generated: {new_token.decode('utf-8').strip()}, Expires at: {datetime.datetime.fromtimestamp(expiration_time)}")
    return new_token, expiration_time

def task(shared_mem_name, lock):
    """Task that checks and refreshes the token if expired."""
    existing_shm = shared_memory.SharedMemory(name=shared_mem_name)
    
    try:
        with lock:
            # Unpack the shared memory buffer into a token and expiration time
            token_bytes, expiration_time = struct.unpack(PACK_FORMAT, existing_shm.buf[:struct.calcsize(PACK_FORMAT)])
            
            # Get current time
            current_time = time.time()
            
            # Check expiration
            if current_time >= expiration_time:
                print(f"Process {multiprocessing.current_process().name} found the token expired, refreshing...")
                
                # Generate a new token and update the shared memory
                new_token, new_expiration = generate_token()
                packed_data = struct.pack(PACK_FORMAT, new_token, new_expiration)
                existing_shm.buf[:struct.calcsize(PACK_FORMAT)] = packed_data
            else:
                print(f"Process {multiprocessing.current_process().name}: Token is still valid, expires at {datetime.datetime.fromtimestamp(expiration_time)}")
    
    finally:
        existing_shm.close()

if __name__ == '__main__':
    # Create shared memory for the token and expiration time
    shm = shared_memory.SharedMemory(create=True, size=struct.calcsize(PACK_FORMAT))
    
    # Initial token creation
    initial_token, initial_expiration = generate_token()
    packed_data = struct.pack(PACK_FORMAT, initial_token, initial_expiration)
    shm.buf[:struct.calcsize(PACK_FORMAT)] = packed_data
    
    # Lock to synchronize processes
    lock = Lock()
    
    try:
        while True:
            # Create a new process every 3 seconds to execute the task method
            process = multiprocessing.Process(target=task, args=(shm.name, lock))
            process.start()
            
            # Wait 3 seconds before starting another process
            time.sleep(3)
            
            # Optionally join the process if you want to wait for its completion
            process.join()
    
    except KeyboardInterrupt:
        print("Stopping processes...")
    
    # Clean up shared memory when the program stops
    shm.close()
    shm.unlink()
