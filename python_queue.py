import queue
import threading

# Create a queue
q = queue.Queue()

def worker():
    while True:
        item = q.get()
        if item is None:
            break  # Exit the loop if a sentinel value is received
        try:
            # Process the item
            print(f'Processing item: {item}')
        finally:
            # Indicate that a formerly enqueued task is complete
            q.task_done()

# Create and start worker threads
num_worker_threads = 4
threads = []
for i in range(num_worker_threads):
    t = threading.Thread(target=worker)
    t.start()
    threads.append(t)

# Add tasks to the queue
for item in range(10):
    q.put(item)

# Block until all tasks in the queue have been processed
q.join()

# Stop workers by putting sentinel values in the queue
for i in range(num_worker_threads):
    q.put(None)

# Wait for all worker threads to exit
for t in threads:
    t.join()
"""
Explanation:
q.put(item): Adds a task to the queue.
q.get(): Retrieves a task from the queue.
q.task_done(): Indicates that a retrieved task is complete. This is crucial when using q.join() to block until all tasks have been processed.
q.join(): Blocks until all items in the queue have been processed (i.e., until task_done() has been called for each item that was put in the queue).
"""

