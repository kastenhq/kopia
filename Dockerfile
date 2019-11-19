FROM kanisterio/kanister-tools:0.20.0

COPY ./kopia /usr/local/bin/kopia

CMD [ "/usr/bin/tail", "-f", "/dev/null" ]
