# Golang stage build
FROM golang:1.13-stretch
WORKDIR /go/src/github.com/databrickslabs/databricks-terraformer/
COPY cgo ./cgo
COPY Makefile .
RUN make shared


FROM python:3.7-stretch
# Setup wokring directory
WORKDIR /databricks-terraformer/
ENV TF_VERSION 0.12.24
ENV GIT_PYTHON_TRACE=full
ENV HOME=/root
RUN curl https://raw.githubusercontent.com/databrickslabs/databricks-terraform/master/godownloader-databricks-provider.sh | bash -s -- -b $HOME/.terraform.d/plugins
RUN wget --quiet https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_linux_amd64.zip \
  && unzip terraform_${TF_VERSION}_linux_amd64.zip \
  && mv terraform /usr/bin \
  && rm terraform_${TF_VERSION}_linux_amd64.zip
COPY --from=0 /go/src/github.com/databrickslabs/databricks-terraformer/databricks_terraformer/hcl/json2hcl.so databricks_terraformer/hcl/json2hcl.so
COPY --from=0 /go/src/github.com/databrickslabs/databricks-terraformer/databricks_terraformer/hcl/json2hcl.h databricks_terraformer/hcl/json2hcl.h
RUN ls . -al
COPY dev-requirements.txt .
RUN pip install -r dev-requirements.txt

COPY . .
RUN pip install .
ENTRYPOINT ["databricks-terraformer"]
