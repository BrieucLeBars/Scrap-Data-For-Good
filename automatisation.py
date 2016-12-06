from threading import *
import subprocess



class ThreadTimer(Thread):
    def __init__(self, event):
        Thread.__init__(self)
        self.stopped = event

    def run(self, content):
        """S'execute toutes les heures"""
        while not self.stopped.wait(3600):
            for element in content:
                cmd_monster = "python3 monster/Scraper.py %s Paris 30" % (element[:-1])
                #cmd_indeed = "python3 indeed/....py ..."
                subprocess.call(cmd_monster, shell = True)


if __name__ == '__main__':


    with open('job_request.txt', 'r') as f:
        content = f.readlines()

    stopFlag = Event()
    thread = ThreadTimer(stopFlag)
    thread.run(content)