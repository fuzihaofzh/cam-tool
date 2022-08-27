import os
import time
pid = os.getpid()
for i in range(40):
    print("Hello world! Hello world! ", pid, i)
    if i % 10 ==9:
        print("========")
    time.sleep(1)