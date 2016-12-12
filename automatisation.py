from threading import *
import subprocess



class ThreadTimer(Thread):
    def __init__(self, event):
        Thread.__init__(self)
        self.stopped = event

    def run(self):
        """S'execute toutes les heures"""
        while not self.stopped.wait(3600):
            cmd = "python3 get_database.py"
            #cmd_indeed = "python3 indeed/....py ..."
            subprocess.call(cmd, shell = True)


if __name__ == '__main__':

    stopFlag = Event()
    thread = ThreadTimer(stopFlag)
    thread.run()