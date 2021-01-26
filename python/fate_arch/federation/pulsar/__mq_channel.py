########################################################
# Copyright 2019-2021 program was created VMware, Inc. #
# SPDX-License-Identifier: Apache-2.0                  #
########################################################

import pulsar
import time
from fate_arch.common import log

LOGGER = log.getLogger()
CHANNEL_TYPE_PRODUCER = 'producer'
CHANNEL_TYPE_CONSUMER = 'consumer'
TOPIC_PREFIX = 'fl-tenant/{}/{}'
UNIQUE_NAME = 'unique'


def connection_retry(func):
    """retry connection
    """

    def wrapper(self, *args, **kwargs):
        """wrapper
        """
        res = None
        for ntry in range(60):
            try:
                res = func(self, *args, **kwargs)
                break
            except Exception as e:
                LOGGER.error("function %s error" %
                             func.__name__, exc_info=True)
                time.sleep(0.1)
        return res
    return wrapper

 # A channel cloud only be able to send or receive message.


class MQChannel(object):
    # TODO add credential to secure pulsar cluster
    def __init__(self, host, port, pulsar_namespace, pulsar_topic, party_id, role, credential=None, extra_args: dict = None):
        # "host:port" is used to connect the pulsar brocker
        self._host = host
        self._port = port
        self._namespace = pulsar_namespace
        # for a topic, eigther a producer or consumer will connection to.
        self._topic = pulsar_topic
        self._credential = credential
        self._party_id = party_id
        self._role = role
        self._extra_args = extra_args

        # "_conn" is a "client" object that handls pulsar connection
        self._conn = None
        # "_channel" is the subscriptor for the topic
        self._producer = None
        self._consumer = None

    @property
    def party_id(self):
        return self._party_id

    @connection_retry
    def basic_publish(self, body, properties):
        self._get_channel()
        LOGGER.debug(f"send queue: {self._send_queue_name}")
        return self._channel.basic_publish(exchange='', routing_key=self._send_queue_name, body=body,
                                           properties=properties)

    @connection_retry
    def consume(self):
        self._get_channel()
        # since consumer and topic are one to one corresponding, maybe it is ok to use unique subsciption name?
        LOGGER.debug('receive topic: {}'.format(consumer.topic()))
        return self._channel.receive(queue=self._receive_queue_name)

    @connection_retry
    def basic_ack(self, delivery_tag):
        self._get_channel()
        return self._channel.basic_ack(delivery_tag=delivery_tag)

    @connection_retry
    def cancel(self):
        self._get_channel()
        return self._channel.close()

    @connection_retry
    def _get_channel(self, _type):
        if self._check_alive():
            return
        else:
            self._clear()

        if not self._conn:
            self._conn = pulsar.Client(
                'pulsar://{}:{}'.format(self._host, self._port))

        # TODO: it is little bit dangerous to pass _extra_args here ;)
        if not self._channel:
            channel = None
            if _type == CHANNEL_TYPE_PRODUCER:
                channel = self._conn.create_producer(TOPIC_PREFIX.format(self._namespace, self._topic),
                                                     producer_name=UNIQUE_NAME,
                                                     **self._extra_args)
            if _type == CHANNEL_TYPE_CONSUMER:
                channel = self._conn.subscribe(TOPIC_PREFIX.format(self._namespace, self._topic),
                                               consumer_name=UNIQUE_NAME,
                                               **self._extra_args)
            self._channel = channel

    def _clear(self):
        try:
            if self._conn is not None:
                self._conn.close()
            self._conn = None
            self._channel = None

        except Exception as e:
            LOGGER.exception(e)
            self._conn = None
            self._channel = None

    def _check_alive(self):
        try:
            self._conn.get_topic_partitions('test-alive')
            return True
        except Exception:
            return False
