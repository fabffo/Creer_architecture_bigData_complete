import schedule
import time
import sys

def job():
   print("I'm working...")

schedule.every(10).seconds.do(job)
#schedule.every().hour.do(job)
#schedule.every().day.at("10:30").do(job)
input = sys.argv[1]
output = sys.argv[2]
job()
while 1:
   schedule.run_pending()
#time.sleep(1)