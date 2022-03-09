import os
import time
pid = os.getpid()
for i in range(40):
    print(pid, i)
    time.sleep(1)