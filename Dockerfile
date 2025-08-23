# image
FROM python:3.11

# env
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# apt-get update
RUN apt-get update && apt-get -y install curl tzdata jq cron
RUN python3 -m pip install --upgrade setuptools

# Copy all app files
COPY . /app/
WORKDIR /app

# terminal settings
RUN echo 'export PS1="\[$(tput bold)\]\[$(tput setaf 6)\]\\t \\d\\n\[$(tput setaf 2)\][\[$(tput setaf 3)\]\u\[$(tput setaf 1)\]@\[$(tput setaf 3)\]\h \[$(tput setaf 6)\]\w\[$(tput setaf 2)\]]\[$(tput setaf 4)\\]\\$ \[$(tput sgr0)\]"' >> /root/.bashrc \
    && echo "alias grep='grep --color=auto'" >> /root/.bashrc

# timezone
RUN ln -snf /usr/share/zoneinfo/Asia/Kuala_Lumpur /etc/localtime && \
    echo Asia/Kuala_Lumpur > /etc/timezone

# install python library
COPY requirements.txt /app/
RUN pip install -r requirements.txt

# docker entrypoint
RUN chmod +x /app/docker-entrypoint.sh
ENTRYPOINT [ "/app/docker-entrypoint.sh" ]
