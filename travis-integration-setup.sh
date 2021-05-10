#!/bin/bash

set -e

OS_VERSION=`lsb_release -c -s`

sudo apt-get install curl gnupg debian-keyring debian-archive-keyring apt-transport-https -y

## Team RabbitMQ's main signing key
sudo apt-key adv --keyserver "hkps://keys.openpgp.org" --recv-keys "0x0A9AF2115F4687BD29803A206B73A36E6026DFCA"
## Cloudsmith: modern Erlang repository
curl -1sLf https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/gpg.E495BB49CC4BBE5B.key | sudo apt-key add -
## Cloudsmith: RabbitMQ repository
curl -1sLf https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/gpg.9F4587F226208342.key | sudo apt-key add -

## Add apt repositories maintained by Team RabbitMQ
sudo tee /etc/apt/sources.list.d/rabbitmq.list <<EOF
## Provides modern Erlang/OTP releases
##
deb https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/deb/ubuntu ${OS_VERSION} main
deb-src https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-erlang/deb/ubuntu ${OS_VERSION} main

## Provides RabbitMQ
##
deb https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/deb/ubuntu ${OS_VERSION} main
deb-src https://dl.cloudsmith.io/public/rabbitmq/rabbitmq-server/deb/ubuntu ${OS_VERSION} main
EOF

## Update package indices
sudo apt-get update -y

## Install Erlang packages
sudo apt-get install -y erlang-base \
                        erlang-asn1 erlang-crypto erlang-eldap erlang-ftp erlang-inets \
                        erlang-mnesia erlang-os-mon erlang-parsetools erlang-public-key \
                        erlang-runtime-tools erlang-snmp erlang-ssl \
                        erlang-syntax-tools erlang-tftp erlang-tools erlang-xmerl

## Install rabbitmq-server and its dependencies
sudo apt-get install rabbitmq-server -y --fix-missing

# Install the management plugin
sudo rabbitmq-plugins enable rabbitmq_management

# Start RabbitMQ
sudo systemctl start rabbitmq-server
