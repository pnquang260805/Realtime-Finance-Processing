FROM python:3.12.12
RUN apt-get update && apt-get install -y cron

WORKDIR /app
COPY . .
RUN pip install requests python-dotenv
# Phải để full path python
# Mặc định dùng thời gian UTC, không phỉa UTC+7
RUN echo "20 14 * * * /usr/local/bin/python3 /app/main.py >> /app/cron.log 2>&1" > /etc/cron.d/my-cron
RUN echo "" >> /etc/cron.d/my-cron
RUN chmod 0644 /etc/cron.d/my-cron
RUN crontab /etc/cron.d/my-cron
RUN touch /app/cron.log

CMD ["sh", "-c", "cron && tail -f /app/cron.log"]