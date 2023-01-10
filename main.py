# -*- encoding: UTF-8 -*-

import utils
import logging
import work_flow
import settings
import schedule
import time


def job():
    from datetime import datetime
    weekday = datetime.today().weekday()
    if weekday < 5:
        work_flow.process()
    elif weekday == 5:
        from strage_evaluate import fresh_evaluate
        fresh_evaluate()


logging.basicConfig(format='%(asctime)s %(message)s', filename='sequoia.log')
logging.getLogger().setLevel(logging.INFO)
settings.init()

if __name__ == '__main__':
    if settings.config['cron']:
        EXEC_TIME = "15:15"
        schedule.every().day.at(EXEC_TIME).do(job)

        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        work_flow.process()
