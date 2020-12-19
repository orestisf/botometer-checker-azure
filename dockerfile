FROM python:3
COPY requirements.txt botometerChecker.py /app/
WORKDIR /app
RUN pip install -r requirements.txt
CMD [ "python", "botometer_checker.py" ]