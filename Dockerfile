FROM python:3.7-stretch
# Setup wokring directory
WORKDIR /databricks-sync/
ENV TF_VERSION 0.14.5
ENV GIT_PYTHON_TRACE=full
ENV HOME=/root
RUN curl https://raw.githubusercontent.com/databrickslabs/databricks-terraform/master/godownloader-databricks-provider.sh | bash -s -- -b $HOME/.terraform.d/plugins
RUN wget --quiet https://releases.hashicorp.com/terraform/${TF_VERSION}/terraform_${TF_VERSION}_linux_amd64.zip \
  && unzip terraform_${TF_VERSION}_linux_amd64.zip \
  && mv terraform /usr/bin \
  && rm terraform_${TF_VERSION}_linux_amd64.zip
RUN ls . -al

RUN git config --global user.email "you@example.com"
RUN git config --global user.name "Your Name"

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN pip install .
ENTRYPOINT ["databricks-sync"]
