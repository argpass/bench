FROM daocloud.io/python:3.6
COPY ./requirements.txt /home/app/requirements.txt
WORKDIR /home/app
RUN pip install -i http://pypi.douban.com/simple --trusted-host pypi.douban.com -r requirements.txt
