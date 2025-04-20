from crontab import CronTab


def main():
    job_scheduler = CronTab(user="user")
    job = job_scheduler.new("python command.py")

    job.minute.every(60)
    job_scheduler.write()


if __name__ == "__main__":
    main()
