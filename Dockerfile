# syntax=docker/dockerfile:1.3
FROM registry.seculayer.com:31500/ape/python-base:py3.7 as builder

ARG app="/opt/app"

RUN pip3.7 install wheel
RUN git config --global http.sslVerify false

# pycmmn setup
# specific branch
RUN --mount=type=secret,id=token git clone -c http.extraHeader="Authorization: Bearer $(cat /run/secrets/token)" -b SLCAI-54-automl-module https://ssdlc-bitbucket.seculayer.com:8443/scm/slaism/autoape-pycmmn.git $app/pycmmn
#RUN --mount=type=secret,id=token git clone -c http.extraHeader="Authorization: Bearer $(cat /run/secrets/token)" https://ssdlc-bitbucket.seculayer.com:8443/scm/slaism/autoape-pycmmn.git $app/pycmmn
WORKDIR $app/pycmmn
RUN pip3.7 install -r requirements.txt -t $app/pycmmn/lib
RUN python3.7 setup.py bdist_wheel

# data-analyzer setup
# specific branch
RUN --mount=type=secret,id=token git clone -c http.extraHeader="Authorization: Bearer $(cat /run/secrets/token)" -b SLCAI-54-automl-module https://ssdlc-bitbucket.seculayer.com:8443/scm/slaism/autoape-da.git $app/da
#RUN --mount=type=secret,id=token git clone -c http.extraHeader="Authorization: Bearer $(cat /run/secrets/token)" https://ssdlc-bitbucket.seculayer.com:8443/scm/slaism/autoape-da.git $app/da
WORKDIR $app/da
RUN pip3.7 install -r requirements.txt -t $app/da/lib
RUN python3.7 setup.py bdist_wheel


FROM registry.seculayer.com:31500/ape/python-base:py3.7 as app

ARG app="/opt/app"
ENV LANG=en_US.UTF-8 LANGUAGE=en_US:en LC_ALL=en_US.UTF-8

# pycmmn install
RUN mkdir -p /eyeCloudAI/app/ape/pycmmn
WORKDIR /eyeCloudAI/app/ape/pycmmn

COPY --from=builder "$app/pycmmn/lib" /eyeCloudAI/app/ape/pycmmn/lib
COPY --from=builder "$app/pycmmn/dist/pycmmn-1.0.0-py3-none-any.whl" \
        /eyeCloudAI/app/ape/pycmmn/pycmmn-1.0.0-py3-none-any.whl

RUN pip3.7 install /eyeCloudAI/app/ape/pycmmn/pycmmn-1.0.0-py3-none-any.whl --no-dependencies  \
    -t /eyeCloudAI/app/ape/pycmmn/ \
    && rm /eyeCloudAI/app/ape/pycmmn/pycmmn-1.0.0-py3-none-any.whl

# data-analyzer install
RUN mkdir -p /eyeCloudAI/app/ape/da
WORKDIR /eyeCloudAI/app/ape/da

COPY ./da.sh /eyeCloudAI/app/ape/da
RUN chmod +x /eyeCloudAI/app/ape/da/da.sh

COPY --from=builder "$app/da/lib" /eyeCloudAI/app/ape/da/lib
COPY --from=builder "$app/da/dist/dataanalyzer-1.0.0-py3-none-any.whl" \
        /eyeCloudAI/app/ape/da/dataanalyzer-1.0.0-py3-none-any.whl

RUN pip3.7 install /eyeCloudAI/app/ape/da/dataanalyzer-1.0.0-py3-none-any.whl --no-dependencies  \
    -t /eyeCloudAI/app/ape/da/ \
    && rm /eyeCloudAI/app/ape/da/dataanalyzer-1.0.0-py3-none-any.whl

RUN groupadd -g 1000 aiuser
RUN useradd -r -u 1000 -g aiuser aiuser
RUN chown -R aiuser:aiuser /eyeCloudAI
USER aiuser

CMD ["/eyeCloudAI/app/ape/da/da.sh", "0", "chief", "0"]
