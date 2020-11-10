import subprocess
import time
processes = []
for i in range(1, 101):
    process = subprocess.Popen(['bash', 'run.sh', '--id', "{}".format(i), '--hosts', 'host100', '--barrier', 'localhost:10000', '--signal', 'localhost:20000', '--output', 'test2', 'dummy'])
    processes.append(process)
time.sleep(10)
for p in processes:
    out, _ = p.communicate()
    print(out)
    p.kill()

    