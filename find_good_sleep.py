#!/usr/bin/env python3

import argparse
import os, atexit
import textwrap
import time
import tempfile
import threading, subprocess
import barrier, finishedSignal
import numpy as np
import sys


import signal
import random
import time
from enum import Enum

from collections import defaultdict, OrderedDict


BARRIER_IP = 'localhost'
BARRIER_PORT = 10000

SIGNAL_IP = 'localhost'
SIGNAL_PORT = 11000

PROCESSES_BASE_IP = 11000

# Do not run multiple validations concurrently!

class TC:
    def __init__(self, losses, interface="lo", needSudo=True, sudoPassword="dcl"):
        self.losses = losses
        self.interface = interface
        self.needSudo = needSudo
        self.sudoPassword = sudoPassword

        cmd1 = 'tc qdisc add dev {} root netem 2>/dev/null'.format(self.interface)
        cmd2 = 'tc qdisc change dev {} root netem delay {} {} distribution normal loss {} {} reorder {} {}'.format(self.interface, *self.losses['delay'], *self.losses['loss'], *self.losses['reordering'])

        if self.needSudo:
            os.system("echo {} | sudo -S {}".format(self.sudoPassword, cmd1))
            os.system("echo {} | sudo -S {}".format(self.sudoPassword, cmd2))
        else:
            os.system(cmd1)
            os.system(cmd2)

        atexit.register(self.cleanup)

    def __str__(self):
        ret = """\
        Interface: {}
          Distribution: Normal
          Delay: {} {}
          Loss: {} {}
          Reordering: {} {}""".format(
              self.interface,
              *self.losses['delay'],
              *self.losses['loss'],
              *self.losses['reordering'])

        return textwrap.dedent(ret)

    def cleanup(self):
        cmd = 'tc qdisc del dev {} root 2>/dev/null'.format(self.interface)
        if self.needSudo:
            os.system("echo '{}' | sudo -S {}".format(self.sudoPassword, cmd))
        else:
            os.system(cmd)

class ProcessState(Enum):
    RUNNING = 1
    STOPPED = 2
    TERMINATED = 3

class ProcessInfo:
    def __init__(self, handle):
        self.lock = threading.Lock()
        self.handle = handle
        self.state = ProcessState.RUNNING

    @staticmethod
    def stateToSignal(state):
        if state == ProcessState.RUNNING:
            return signal.SIGCONT

        if state == ProcessState.STOPPED:
            return signal.SIGSTOP

        if state == ProcessState.TERMINATED:
            return signal.SIGTERM

    @staticmethod
    def stateToSignalStr(state):
        if state == ProcessState.RUNNING:
            return "SIGCONT"

        if state == ProcessState.STOPPED:
            return "SIGSTOP"

        if state == ProcessState.TERMINATED:
            return "SIGTERM"

    @staticmethod
    def validStateTransition(current, desired):
        if current == ProcessState.TERMINATED:
            return False

        if current == ProcessState.RUNNING:
            return desired == ProcessState.STOPPED or desired == ProcessState.TERMINATED

        if current == ProcessState.STOPPED:
            return desired == ProcessState.RUNNING

        return False

class AtomicSaturatedCounter:
    def __init__(self, saturation, initial=0):
        self._saturation = saturation
        self._value = initial
        self._lock = threading.Lock()

    def reserve(self):
        with self._lock:
            if self._value < self._saturation:
                self._value += 1
                return True
            else:
                return False

class Validation:
    def __init__(self, processes, messages, outputDir):
        self.processes = processes
        self.messages = messages
        self.outputDirPath = os.path.abspath(outputDir)
        if not os.path.isdir(self.outputDirPath):
            raise Exception("`{}` is not a directory".format(self.outputDirPath))

    def generateConfig(self):
        # Implement on the derived classes
        pass

    def checkProcess(self, pid):
        # Implement on the derived classes
        pass

    def checkAll(self, continueOnError=True):
        ok = True
        for pid in range(1, self.processes+1):
            ret = self.checkProcess(pid)
            if not ret:
                ok = False

            if not ret and not continueOnError:
                return False

        return ok

class FifoBroadcastValidation(Validation):
    def generateConfig(self):
        hosts = tempfile.NamedTemporaryFile(mode='w')
        config = tempfile.NamedTemporaryFile(mode='w')

        for i in range(1, self.processes + 1):
            hosts.write("{} localhost {}\n".format(i, PROCESSES_BASE_IP+i))

        hosts.flush()

        config.write("{}\n".format(self.messages))
        config.flush()

        return (hosts, config)

    def checkProcess(self, pid):
        filePath = os.path.join(self.outputDirPath, 'proc{:02d}.output'.format(pid))

        i = 1
        nextMessage = defaultdict(lambda : 1)
        filename = os.path.basename(filePath)

        with open(filePath) as f:
            for lineNumber, line in enumerate(f):
                tokens = line.split()

                # Check broadcast
                if tokens[0] == 'b':
                    msg = int(tokens[1])
                    if msg != i:
                        print("File {}, Line {}: Messages broadcast out of order. Expected message {} but broadcast message {}".format(filename, lineNumber, i, msg), flush=True)
                        return False
                    i += 1

                # Check delivery
                if tokens[0] == 'd':
                    sender = int(tokens[1])
                    msg = int(tokens[2])
                    if msg != nextMessage[sender]:
                        print("File {}, Line {}: Message delivered out of order. Expected message {}, but delivered message {}".format(filename, lineNumber, nextMessage[sender], msg), flush=True)
                        return False
                    else:
                        nextMessage[sender] = msg + 1

        return True

class LCausalBroadcastValidation(Validation):
    def __init__(self, processes, outputDir, causalRelationships):
        super().__init__(processes, outputDir)

    def generateConfig(self):
        raise NotImplementedError()

    def checkProcess(self, pid):
        raise NotImplementedError()

class StressTest:
    def __init__(self, procs, concurrency, attempts, attemptsRatio):
        self.processes = len(procs)
        self.processesInfo = dict()
        for (logicalPID, handle) in procs:
            self.processesInfo[logicalPID] = ProcessInfo(handle)
        self.concurrency = concurrency
        self.attempts = attempts
        self.attemptsRatio = attemptsRatio

        maxTerminatedProcesses = self.processes // 2 if self.processes % 2 == 1 else (self.processes - 1) // 2
        self.terminatedProcs = AtomicSaturatedCounter(maxTerminatedProcesses)

    def stress(self):
        selectProc = list(range(1, self.processes+1))
        random.shuffle(selectProc)

        selectOp = [ProcessState.STOPPED] * int(1000 * self.attemptsRatio['STOP']) + \
                    [ProcessState.RUNNING] * int(1000 * self.attemptsRatio['CONT']) + \
                    [ProcessState.TERMINATED] * int(1000 * self.attemptsRatio['TERM'])
        random.shuffle(selectOp)

        successfulAttempts = 0
        while successfulAttempts < self.attempts:
            proc = random.choice(selectProc)
            op = random.choice(selectOp)
            info = self.processesInfo[proc]

            with info.lock:
                if ProcessInfo.validStateTransition(info.state, op):

                    if op == ProcessState.TERMINATED:
                        reserved = self.terminatedProcs.reserve()
                        if reserved:
                            selectProc.remove(proc)
                        else:
                            continue

                    time.sleep(float(random.randint(50, 500)) / 1000.0)
                    info.handle.send_signal(ProcessInfo.stateToSignal(op))
                    info.state = op
                    successfulAttempts += 1
                    print("Sending {} to process {}".format(ProcessInfo.stateToSignalStr(op), proc), flush=True)

                    # if op == ProcessState.TERMINATED and proc not in terminatedProcs:
                    #     if len(terminatedProcs) < maxTerminatedProcesses:

                    #         terminatedProcs.add(proc)

                    # if len(terminatedProcs) == maxTerminatedProcesses:
                    #     break

    def remainingUnterminatedProcesses(self):
        remaining = []
        for pid, info in self.processesInfo.items():
            with info.lock:
                if info.state != ProcessState.TERMINATED:
                    remaining.append(pid)

        return None if len(remaining) == 0 else remaining

    def terminateAllProcesses(self):
        for _, info in self.processesInfo.items():
            with info.lock:
                if info.state != ProcessState.TERMINATED:
                    if info.state == ProcessState.STOPPED:
                        info.handle.send_signal(ProcessInfo.stateToSignal(ProcessState.RUNNING))

                    info.handle.send_signal(ProcessInfo.stateToSignal(ProcessState.TERMINATED))

        return False

    def continueStoppedProcesses(self):
        for _, info in self.processesInfo.items():
            with info.lock:
                if info.state != ProcessState.TERMINATED:
                    if info.state == ProcessState.STOPPED:
                        info.handle.send_signal(ProcessInfo.stateToSignal(ProcessState.RUNNING))

    def run(self):
        if self.concurrency > 1:
            threads = [threading.Thread(target=self.stress) for _ in range(self.concurrency)]
            [p.start() for p in threads]
            [p.join() for p in threads]
        else:
            self.stress()

def startProcesses(processes, runscript, hostsFilePath, configFilePath, outputDir, 
                    sleepSend, sleepDeliver, timeoutAck, tri, numOutstanding, sleepOutStanding):
    runscriptPath = os.path.abspath(runscript)
    if not os.path.isfile(runscriptPath):
        raise Exception("`{}` is not a file".format(runscriptPath))

    if os.path.basename(runscriptPath) != 'run.sh':
        raise Exception("`{}` is not a runscript".format(runscriptPath))

    outputDirPath = os.path.abspath(outputDir)
    if not os.path.isdir(outputDirPath):
        raise Exception("`{}` is not a directory".format(outputDirPath))

    baseDir, _ = os.path.split(runscriptPath)
    bin_cpp = os.path.join(baseDir, "bin", "da_proc")
    bin_java = os.path.join(baseDir, "bin", "da_proc.jar")

    if os.path.exists(bin_cpp):
        cmd = [bin_cpp]
    elif os.path.exists(bin_java):
        cmd = ['java', '-jar', bin_java]
    else:
        raise Exception("`{}` could not find a binary to execute. Make sure you build before validating".format(runscriptPath))

    procs = []
    for pid in range(1, processes+1):
        cmd_ext = ['--id', str(pid),
                   '--hosts', hostsFilePath,
                   '--barrier', '{}:{}'.format(BARRIER_IP, BARRIER_PORT),
                   '--signal', '{}:{}'.format(SIGNAL_IP, SIGNAL_PORT),
                   '--output', os.path.join(outputDirPath, 'proc{:02d}.output'.format(pid)),
                   configFilePath, str(sleepSend), str(sleepDeliver), str(timeoutAck), str(tri), str(numOutstanding), str(sleepOutStanding)]

        stdoutFd = open(os.path.join(outputDirPath, 'proc{:02d}.stdout'.format(pid)), "w")
        stderrFd = open(os.path.join(outputDirPath, 'proc{:02d}.stderr'.format(pid)), "w")


        procs.append((pid, subprocess.Popen(cmd + cmd_ext, stdout=stdoutFd, stderr=stderrFd)))

    return procs

def main(processes, messages, runscript, broadcastType, logsDir, testConfig):
    # Set tc for loopback
    #tc = TC(testConfig['TC'])
    #print(tc)

    sleepSends = [25, 50]
    sleepDelivers = [150, 200, 250, 300, 350]
    timeoutAcks = [10, 15]
    tries = [2]
    numOutstandings = [1400, 1500, 1600]
    sleepOutStandings = [50, 100, 150]

    best_finish = float('inf')
    best_del = 0
    best_finish_dict = defaultdict(int)
    best_del_dict = defaultdict(int)

    for sleepSend in sleepSends:
        for sleepDeliver in sleepDelivers:
            for timeoutAck in timeoutAcks:
                for tri in tries:
                    for numOutstanding in numOutstandings:
                        for sleepOutStanding in sleepOutStandings:
                            print("sleepSend:", sleepSend, flush=True)
                            print("sleepDeliver:", sleepDeliver, flush=True)
                            print("timeoutAck:", timeoutAck, flush=True)
                            print("tries:", tri, flush=True)
                            print("numOutstanding:", numOutstanding, flush=True)
                            print("sleepOutStanding:", sleepOutStanding, flush=True)
                            tot_finishes = []
                            tot_dels = []
                            for _ in range(3):
                                # Start the barrier
                                initBarrier = barrier.Barrier(BARRIER_IP, BARRIER_PORT, processes)
                                initBarrier.listen()
                                startTimesFuture = initBarrier.startTimesFuture()

                                initBarrierThread = threading.Thread(target=initBarrier.wait)
                                initBarrierThread.start()

                                # Start the finish signal
                                finishSignal = finishedSignal.FinishedSignal(SIGNAL_IP, SIGNAL_PORT, processes)
                                finishSignal.listen()
                                finishSignalThread = threading.Thread(target=finishSignal.wait)
                                finishSignalThread.start()

                                if broadcastType == "fifo":
                                    validation = FifoBroadcastValidation(processes, messages, logsDir)
                                else:
                                    validation = LCausalBroadcastValidation(processes, messages, logsDir, None)

                                hostsFile, configFile = validation.generateConfig()

                                try:
                                    # Start the processes and get their PIDs
                                    procs = startProcesses(processes, runscript, hostsFile.name, configFile.name, logsDir, 
                                                            sleepSend, sleepDeliver, timeoutAck, tri, numOutstanding, sleepOutStanding)

                                    # Create the stress test
                                    st = StressTest(procs,
                                                    testConfig['ST']['concurrency'],
                                                    testConfig['ST']['attempts'],
                                                    testConfig['ST']['attemptsDistribution'])

                                    for (logicalPID, procHandle) in procs:
                                        print("Process with logicalPID {} has PID {}".format(logicalPID, procHandle.pid), flush=True)


                                    initBarrierThread.join()
                                    print("All processes have been initialized.", flush=True)

                                    # st.run()
                                    # print("StressTest is complete.")


                                    # print("Resuming stopped processes.")
                                    # st.continueStoppedProcesses()

                                    print("Waiting until all running processes have finished broadcasting.", flush=True)
                                    finishSignalThread.join(1)

                                    if(not finishSignalThread.is_alive()):
                                        finishes = []
                                        for pid, startTs in OrderedDict(sorted(startTimesFuture.items())).items():
                                            print("Process {} finished broadcasting {} messages in {} ms".format(pid, messages, finishSignal.endTimestamps()[pid] - startTs), flush=True)
                                            finishes.append(finishSignal.endTimestamps()[pid] - startTs)
                                        avg_time = np.mean(np.array(finishes))
                                        print("Average time to finished broadcast: {} ms".format(avg_time), flush=True)
                                        tot_finishes.append(avg_time)
                                    else:
                                        tot_finishes.append(1000)
                                        print("Average time to finished broadcast: > 1 seconds", flush=True)


                                    numberDel = []
                                    time.sleep(10)
                                    st.terminateAllProcesses()
                                    time.sleep(3)
                                    for pid in range(1, processes+1):
                                        filePath = os.path.join(logsDir, 'proc{:02d}.stdout'.format(pid))
                                        with open(filePath) as f:
                                            for line in f:
                                                if "Total message delivered: " in line:
                                                    tot_del = line.split("Total message delivered: ")[-1].rstrip("\n").rstrip()
                                                    print("Process {} delivered {} messages".format(pid, tot_del), flush=True)
                                                    numberDel.append(int(tot_del))
                                                    break
                                    avg_del = np.mean(np.array(numberDel))
                                    print("Average number of delivered messages: {} ".format(avg_del), flush=True)
                                    tot_dels.append(avg_del)

                                    mutex = threading.Lock()

                                    def waitForProcess(logicalPID, procHandle, mutex):
                                        procHandle.wait()
                                        with mutex:
                                            print("Process {} exited with {}".format(logicalPID, procHandle.returncode), flush=True)

                                    # Monitor which processes have exited
                                    monitors = [threading.Thread(target=waitForProcess, args=(logicalPID, procHandle, mutex)) for (logicalPID, procHandle) in procs]
                                    [p.start() for p in monitors]
                                    [p.join() for p in monitors]
                                    if procs is not None:
                                        for _, p in procs:
                                            p.kill()

                                    # input('Hit `Enter` to validate the output')
                                    # print("Result of validation: {}".format(validation.checkAll()))
                                finally:
                                    if procs is not None:
                                        for _, p in procs:
                                            p.kill()
                            full_avg_del = np.mean(np.array(tot_dels))
                            if (full_avg_del > best_del):
                                best_del_dict["sleepSend"] = sleepSend
                                best_del_dict["sleepDeliver"] = sleepDeliver
                                best_del_dict["timeoutAck"] = timeoutAck
                                best_del_dict["tries"] = tri
                                best_del_dict["numOutstanding"] = numOutstanding
                                best_del_dict["sleepOutStanding"] = sleepOutStanding
                                print("Best del dict:", best_del_dict, flush=True)
                                print("Best del:", full_avg_del, flush=True)
                                best_del = full_avg_del
                            full_avg_finishes = np.mean(np.array(tot_finishes))
                            if (full_avg_finishes < best_finish):
                                best_finish_dict["sleepSend"] = sleepSend
                                best_finish_dict["sleepDeliver"] = sleepDeliver
                                best_finish_dict["timeoutAck"] = timeoutAck
                                best_finish_dict["tries"] = tri
                                best_finish_dict["numOutstanding"] = numOutstanding
                                best_finish_dict["sleepOutStanding"] = sleepOutStanding
                                best_finish = full_avg_finishes
                                print("Best finish dict:", best_finish_dict, flush=True)
                                print("Best finish:", full_avg_finishes, flush=True)

    print("Best del num: ", best_del, flush=True)
    print("Best del dict:", best_del_dict, flush=True)
    print("Best finish time: ", best_finish, flush=True)
    print("Best finish dict:", best_finish_dict, flush=True)

if __name__ == "__main__":
    sys.stdout = open("param.log", "w")
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-r",
        "--runscript",
        required=True,
        dest="runscript",
        help="Path to run.sh",
    )

    parser.add_argument(
        "-b",
        "--broadcast",
        choices=["fifo", "lcausal"],
        required=True,
        dest="broadcastType",
        help="Which broadcast implementation to test",
    )

    parser.add_argument(
        "-l",
        "--logs",
        required=True,
        dest="logsDir",
        help="Directory to store stdout, stderr and outputs generated by the processes",
    )

    parser.add_argument(
        "-p",
        "--processes",
        required=True,
        type=int,
        dest="processes",
        help="Number of processes that broadcast",
    )

    parser.add_argument(
        "-m",
        "--messages",
        required=True,
        type=int,
        dest="messages",
        help="Maximum number (because it can crash) of messages that each process can broadcast",
    )

    results = parser.parse_args()

    testConfig = {
        # # Network configuration using the tc command
        # 'TC': {
        #     'delay': ('100ms', '25ms'),
        #     'loss': ('5%', '10%'),
        #     'reordering': ('10%', '20%')
        # },

        # # StressTest configuration
        # 'ST': {
        #     'concurrency' : , # How many threads are interferring with the running processes
        #     'attempts' : 8, # How many interferring attempts each threads does
        #     'attemptsDistribution' : { # Probability with which an interferring thread will
        #         'STOP': 0.48,          # select an interferring action (make sure they add up to 1)
        #         'CONT': 0.48,
        #         'TERM':0.04
        #     }
        # }
        
        # No stress
        # Network configuration using the tc command
        'TC': {
            'delay': ('0ms', '0ms'),
            'loss': ('0%', '0%'),
            'reordering': ('0%', '0%')
        },

        # StressTest configuration
        'ST': {
            'concurrency' : 0, # How many threads are interferring with the running processes
            'attempts' : 0, # How many interferring attempts each threads does
            'attemptsDistribution' : { # Probability with which an interferring thread will
                'STOP': 0,          # select an interferring action (make sure they add up to 1)
                'CONT': 0,
                'TERM':0
            }
        }
    }

    main(results.processes, results.messages, results.runscript, results.broadcastType, results.logsDir, testConfig)
