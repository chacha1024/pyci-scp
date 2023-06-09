FROM python:3.9-slim-buster

COPY . /app
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
WORKDIR /app
RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
	&& echo "Asia/Shanghai" > /etc/timezone \
	&& pip install --upgrade pip \
	--no-cache-dir \
	&& pip install -r requirements.txt \
	--no-cache-dir


ENTRYPOINT ["/entrypoint.sh"]