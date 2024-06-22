from multiprocessing import Process, Queue

def worker(q):
    while True:
        item = q.get()
        if item is None:
            break
        print(f'Processing item: {item}')

q = Queue()
p = Process(target=worker, args=(q,))
p.start()

for item in range(10):
    q.put(item)

q.put(None)  # Sentinel to signal the worker to exit
p.join()
