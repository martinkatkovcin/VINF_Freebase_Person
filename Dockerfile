FROM coady/pylucene:9.1

WORKDIR /usr/app/src

COPY . .

CMD ["python3", "-m", "http.server", "80"]