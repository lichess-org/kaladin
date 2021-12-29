ARG TARGET

FROM tensorflow/tensorflow:2.4.1$TARGET-jupyter

RUN apt-get update

# create Kaladin user
RUN useradd kaladin && \
apt-get install sudo && \
usermod -aG sudo kaladin && \
# Disable sudo login for the new kaladin user.
echo "kaladin ALL = NOPASSWD : ALL" >> /etc/sudoers

USER kaladin

WORKDIR /home/kaladin
RUN sudo chown -R kaladin /home/kaladin

# Update pip: https://packaging.python.org/tutorials/installing-packages/#ensure-pip-setuptools-and-wheel-are-up-to-date
RUN sudo -H python3 -m pip install --upgrade pip setuptools wheel

# By copying over requirements first, we make sure that Docker will "cache"
# our installed requirements in a dedicated FS layer rather than reinstall
# them on every build
COPY --chown=kaladin requirements.txt requirements.txt

# Install the requirements
RUN sudo -H python3 -m pip install -r requirements.txt

COPY --chown=kaladin src src

# Only needed for Jupyter
EXPOSE 8888

# path fix
ENV PYTHONPATH="$PYTHONPATH:~/kaladin"

ENTRYPOINT bash
CMD ["cd src", "python3 queue_manager.py"]