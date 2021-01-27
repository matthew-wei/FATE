########################################################
# Copyright 2019-2021 program was created VMware, Inc. #
# SPDX-License-Identifier: Apache-2.0                  #
########################################################

import io
import json
import sys
import time
import typing
from pickle import dumps as p_dumps, loads as p_loads

import pulsar
# noinspection PyPackageRequirements
from pyspark import SparkContext, RDD

from fate_arch.common import conf_utils, file_utils
from fate_arch.abc import FederationABC, GarbageCollectionABC
from fate_arch.common import Party
from fate_arch.common.log import getLogger
from fate_arch.computing.spark import get_storage_level, Table
from fate_arch.computing.spark._materialize import materialize
from fate_arch.federation.pulsar._mq_channel import MQChannel, DEFAULT_TENANT, DEFAULT_CLUSTER
from fate_arch.federation.pulsar._pulsar_manager import PulsarManager


LOGGER = getLogger()

NAME_DTYPE_TAG = '<dtype>'
_SPLIT_ = '^'


class FederationDataType(object):
    OBJECT = 'obj'
    TABLE = 'Table'


class MQ(object):
    def __init__(self, host, port, route_table):
        self.host = host
        self.port = port
        self.route_table = route_table

    def __str__(self):
        return f"MQ(host={self.host}, port={self.port}, union_name={self.union_name}, " \
               f"policy_id={self.policy_id}, route_table={self.route_table}), " \
               f"type=pulsar"

    def __repr__(self):
        return self.__str__()


class _TopicPair(object):
    def __init__(self, namespace, send, receive):
        self.namespace = namespace
        self.send = send
        self.receive = receive


class Federation(FederationABC):

    @staticmethod
    def from_conf(federation_session_id: str,
                  party: Party,
                  runtime_conf: dict,
                  pulsar_config: dict):
        LOGGER.debug(f"pulsar_config: {pulsar_config}")
        host = pulsar_config.get("host")
        port = pulsar_config.get("port")
        mng_port = pulsar_config.get("mng_port")
        # pulsar not use user and password so far

        # TODO add credential to connections
        base_user = pulsar_config.get('user')
        base_password = pulsar_config.get('password')

        pulsar_manager = PulsarManager(host, mng_port)
        # init tenant
        pulsar_manager.create_tenant(tenant=DEFAULT_TENANT)

        route_table_path = pulsar_config.get("route_table")
        if route_table_path is None:
            route_table_path = "conf/pulsar_route_table.yaml"
        route_table = file_utils.load_yaml_conf(conf_path=route_table_path)
        mq = MQ(host, port, route_table)
        return Federation(federation_session_id, party, mq, pulsar_manager)

    def __init__(self, session_id, party: Party, mq: MQ, pulsar_manager: PulsarManager):
        self._session_id = session_id
        self._party = party
        self._mq = mq
        self._pulsar_manager = pulsar_manager

        self._topic_map: typing.MutableMapping[_TopicKey, _TopicPair] = {}
        self._channels_map: typing.MutableMapping[_TopicKey, MQChannel] = {}

        self._name_dtype_map = {}
        self._message_cache = {}

    def __getstate__(self):
        pass

    def get(self, name: str, tag: str, parties: typing.List[Party], gc: GarbageCollectionABC) -> typing.List:
        log_str = f"[pulsar.get](name={name}, tag={tag}, parties={parties})"
        LOGGER.debug(f"[{log_str}]start to get")

        _name_dtype_keys = [_SPLIT_.join(
            [party.role, party.party_id, name]) for party in parties]

        if _name_dtype_keys[0] not in self._name_dtype_map:
            mq_names = self._get_topic_pairs(parties, dtype=NAME_DTYPE_TAG)
            channel_infos = self._get_channels(mq_names=mq_names)
            rtn_dtype = []
            for i, info in enumerate(channel_infos):
                obj = self._receive_obj(info, name, tag=NAME_DTYPE_TAG)
                rtn_dtype.append(obj)
                LOGGER.debug(f"[pulsar.get] name: {name}, dtype: {obj}")

            for k in _name_dtype_keys:
                if k not in self._name_dtype_map:
                    self._name_dtype_map[k] = rtn_dtype[0]

        rtn_dtype = self._name_dtype_map[_name_dtype_keys[0]]

        rtn = []
        dtype = rtn_dtype.get("dtype", None)
        partitions = rtn_dtype.get("partitions", None)

        if dtype == FederationDataType.TABLE:
            mq_names = self._get_topic_pairs(parties, name, partitions=partitions)
            for i in range(len(mq_names)):
                party = parties[i]
                role = party.role
                party_id = party.party_id
                party_mq_names = mq_names[i]
                receive_func = self._get_partition_receive_func(name, tag, party_id, role, party_mq_names, mq=self._mq,
                                                                connection_conf=self._rabbit_manager.runtime_config.get('connection', {}))

                sc = SparkContext.getOrCreate()
                rdd = sc.parallelize(range(partitions), partitions)
                rdd = rdd.mapPartitionsWithIndex(receive_func)
                rdd = materialize(rdd)
                table = Table(rdd)
                rtn.append(table)
                # add gc
                gc.add_gc_action(tag, table, '__del__', {})

                LOGGER.debug(
                    f"[{log_str}]received rdd({i + 1}/{len(parties)}), party: {parties[i]} ")
        else:
            mq_names = self._get_topic_pairs(parties, name)
            channel_infos = self._get_channels(mq_names=mq_names)
            for i, info in enumerate(channel_infos):
                obj = self._receive_obj(info, name, tag)
                LOGGER.debug(
                    f"[{log_str}]received obj({i + 1}/{len(parties)}), party: {parties[i]} ")
                rtn.append(obj)

        LOGGER.debug(f"[{log_str}]finish to get")
        return rtn

    def remote(self, v, name: str, tag: str, parties: typing.List[Party],
               gc: GarbageCollectionABC) -> typing.NoReturn:
        log_str = f"[pulsar.remote](name={name}, tag={tag}, parties={parties})"

        _name_dtype_keys = [_SPLIT_.join(
            [party.role, party.party_id, name]) for party in parties]

        if _name_dtype_keys[0] not in self._name_dtype_map:
            mq_names = self._get_topic_pairs(parties, dtype=NAME_DTYPE_TAG)
            channel_infos = self._get_channels(mq_names=mq_names)
            if isinstance(v, Table):
                body = {"dtype": FederationDataType.TABLE,
                        "partitions": v.partitions}
            else:
                body = {"dtype": FederationDataType.OBJECT}

            LOGGER.debug(
                f"[pulsar.remote] _name_dtype_keys: {_name_dtype_keys}, dtype: {body}")
            self._send_obj(name=name, tag=NAME_DTYPE_TAG,
                           data=p_dumps(body), channel_infos=channel_infos)

            for k in _name_dtype_keys:
                if k not in self._name_dtype_map:
                    self._name_dtype_map[k] = body

        if isinstance(v, Table):
            total_size = v.count()
            partitions = v.partitions
            LOGGER.debug(
                f"[{log_str}]start to remote RDD, total_size={total_size}, partitions={partitions}")

            mq_names = self._get_topic_pairs(parties, name, partitions=partitions)
            # add gc
            gc.add_gc_action(tag, v, '__del__', {})

            send_func = self._get_partition_send_func(name, tag, partitions, mq_names, mq=self._mq,
                                                      maximun_message_size=self._max_message_size,
                                                      connection_conf=self._rabbit_manager.runtime_config.get('connection', {}))
            # noinspection PyProtectedMember
            v._rdd.mapPartitionsWithIndex(send_func).count()
        else:
            LOGGER.debug(f"[{log_str}]start to remote obj")
            mq_names = self._get_topic_pairs(parties, name)
            channel_infos = self._get_channels(mq_names=mq_names)
            self._send_obj(name=name, tag=tag, data=p_dumps(v),
                           channel_infos=channel_infos)

        LOGGER.debug(f"[{log_str}]finish to remote")

    def cleanup(self, parties):
        LOGGER.debug("[pulsar.cleanup]start to cleanup...")
        for party in parties:
            vhost = self._get_vhost(party)
            LOGGER.debug(
                f"[pulsar.cleanup]start to cleanup vhost {vhost}...")
            self._rabbit_manager.delete_vhost(vhost=vhost)
            LOGGER.debug(f"[pulsar.cleanup]cleanup vhost {vhost} done")
        if self._mq.union_name:
            LOGGER.debug(
                f"[pulsar.cleanup]clean user {self._mq.union_name}.")
            self._rabbit_manager.delete_user(user=self._mq.union_name)

    def _get_vhost(self, party):
        low, high = (self._party, party) if self._party < party else (
            party, self._party)
        vhost = f"{self._session_id}-{low.role}-{low.party_id}-{high.role}-{high.party_id}"
        return vhost

    def _get_topic_pairs(self, parties: typing.List[Party], name=None, partitions=None, dtype=None) -> typing.List:
        mq_names = [self._get_or_create_topic(
            party, name, partitions, dtype) for party in parties]
        return mq_names

    def _get_or_create_topic(self, party: Party, name=None, partitions=None, dtype=None, client_type=None) -> typing.Tuple:
        topic_key_list = []
        topic_infos = []

        if dtype is not None:
            topic_key = _SPLIT_.join(
                [party.role, party.party_id, dtype, dtype])
            topic_key_list.append(topic_key)
        else:
            if partitions is not None:
                for i in range(partitions):
                    topic_key = _SPLIT_.join(
                        [party.role, party.party_id, name, str(i)])
                    topic_key_list.append(topic_key)
            elif name is not None:
                topic_key = _SPLIT_.join([party.role, party.party_id, name])
                topic_key_list.append(topic_key)
            else:
                topic_key = _SPLIT_.join([party.role, party.party_id])
                topic_key_list.append(topic_key)

        for topic_key in topic_key_list:
            if topic_key not in self._topic_map:
                LOGGER.debug(
                    f"[pulsar.get_or_create_topic]topic: {topic_key} for party:{party} not found, start to create")
                # gen names

                topic_key_splits = topic_key.split(_SPLIT_)
                queue_suffix = "-".join(topic_key_splits[2:])
                send_topic_name = f"{self._party.role}-{self._party.party_id}-{party.role}-{party.party_id}-{queue_suffix}"
                receive_topic_name = f"{party.role}-{party.party_id}-{self._party.role}-{self._party.party_id}-{queue_suffix}"

                # topic_pair is a pair of topic for sending and receiving message respectively
                topic_pair = _TopicPair(namespace = self._session_id, send=send_topic_name, receive=receive_topic_name)

                # init pulsar cluster
                cluster = self._pulsar_manager.get_cluster(
                    party.party_id).json()
                if cluster['brokerServiceUrl'] is None or cluster['brokerServiceUrlTls'] is None:
                    LOGGER.debug(
                        "pulsar cluster with name %s does not exist, creating...", party.party_id)
                    host = self._mq.route_table.get(
                        int(party.party_id)).get("host")
                    port = self._mq.route_table.get(
                        int(party.party_id)).get("port")
                    broker_url = f"{host}:{port}"

                    if self._pulsar_manager.create_cluster(cluster_name=party.party_id, broker_url=broker_url).ok:
                        LOGGER.debug(
                            "pulsar cluster with name: %s, broker_url: %s crated", party.party_id, broker_url)
                    else:
                        error_message = "unable to create pulsar cluster: %s".format(
                            party.party_id)
                        LOGGER.error(error_message)
                        # I amd little bit torn here, so I just decide to leave this alone.
                        raise Exception(error_message)

                # update tenant
                tenant_info = self._pulsar_manager.get_tenant(
                    DEFAULT_TENANT).json()
                if party.party_id not in tenant_info['allowedClusters']:
                    tenant_info['allowedClusters'].append(party.party_id)
                    if self._pulsar_manager.update_tenant(DEFAULT_TENANT, tenant_info['admins'], tenant_info['allowedClusters']).ok:
                       LOGGER.debug(
                           'successfully update tenant with cluster: %s', party.party_id)
                    else:
                        raise Exception('unable to update tenant')

                # init pulsar namespace
                if self._pulsar_manager.create_namespace(DEFAULT_TENANT, self._session_id).ok:
                    LOGGER.debug(
                        "successfully create pulsar namespace: %s", self._session_id)
                else:
                    raise Exception("unable to create pulsar namespace")

                self._topic_map[topic_key] = topic_pair
                # TODO: check federated queue status
                LOGGER.debug(
                    f"[pulsar.get_or_create_topic]topic for topic_key: {topic_key}, party:{party} created")

            topic_pairs = self._topic_map[topic_key]
            topic_infos.append((topic_key, topic_pairs))

        return topic_infos


    def _get_channel(self, mq, queue_names: _QueueNames, party_id, role, connection_conf: dict):
        return MQChannel(host=mq.host, port=mq.port, user=mq.union_name, password=mq.policy_id,
                         vhost=queue_names.vhost, send_queue_name=queue_names.send, receive_queue_name=queue_names.receive,
                         party_id=party_id, role=role, extra_args=connection_conf)

    def _get_channels(self, mq_names):
        channel_infos = []
        for e in mq_names:
            for queue_key, queue_names in e:
                queue_key_splits = queue_key.split(_SPLIT_)
                role = queue_key_splits[0]
                party_id = queue_key_splits[1]
                info = self._channels_map.get(queue_key)
                if info is None:
                    info = self._get_channel(self._mq, queue_names, party_id=party_id, role=role,
                                             connection_conf=self._rabbit_manager.runtime_config.get('connection', {}))
                    self._channels_map[queue_key] = info
                channel_infos.append(info)
        return channel_infos

    # can't pickle _thread.lock objects
    def _get_channels_index(self, index, mq_names, mq, connection_conf: dict):
        channel_infos = []
        for e in mq_names:
            queue_key, queue_names = e[index]
            queue_key_splits = queue_key.split(_SPLIT_)
            role = queue_key_splits[0]
            party_id = queue_key_splits[1]
            info = self._get_channel(
                mq, queue_names, party_id=party_id, role=role, connection_conf=connection_conf)
            channel_infos.append(info)
        return channel_infos

    def _send_obj(self, name, tag, data, channel_infos):
        for info in channel_infos:
            properties = pika.BasicProperties(
                content_type='text/plain',
                app_id=info.party_id,
                message_id=name,
                correlation_id=tag,
                delivery_mode=1
            )
            LOGGER.debug(f"[pulsar._send_obj]properties:{properties}.")
            info.basic_publish(body=data, properties=properties)

    def _get_message_cache_key(self, name, tag, party_id, role):
        cache_key = _SPLIT_.join([name, tag, str(party_id), role])
        return cache_key

    def _receive_obj(self, channel_info, name, tag):
        party_id = channel_info._party_id
        role = channel_info._role
        wish_cache_key = self._get_message_cache_key(name, tag, party_id, role)

        if wish_cache_key in self._message_cache:
            return self._message_cache[wish_cache_key]

        for method, properties, body in channel_info.consume():
            LOGGER.debug(
                f"[pulsar._receive_obj] method: {method}, properties: {properties}.")
            if properties.message_id != name or properties.correlation_id != tag:
                # todo: fix this
                LOGGER.warning(
                    f"[pulsar._receive_obj] require {name}.{tag}, got {properties.message_id}.{properties.correlation_id}")

            cache_key = self._get_message_cache_key(
                properties.message_id, properties.correlation_id, party_id, role)
            # object
            if properties.content_type == 'text/plain':
                self._message_cache[cache_key] = p_loads(body)
                channel_info.basic_ack(delivery_tag=method.delivery_tag)
                if cache_key == wish_cache_key:
                    channel_info.cancel()
                    LOGGER.debug(
                        f"[pulsar._receive_obj] cache_key: {cache_key}, obj: {self._message_cache[cache_key]}")
                    return self._message_cache[cache_key]
            else:
                raise ValueError(
                    f"[pulsar._receive_obj] properties.content_type is {properties.content_type}, but must be text/plain")

    def _send_kv(self, name, tag, data, channel_infos, partition_size, partitions, message_key):
        headers = {"partition_size": partition_size,
                   "partitions": partitions, "message_key": message_key}
        for info in channel_infos:
            properties = pika.BasicProperties(
                content_type='application/json',
                app_id=info.party_id,
                message_id=name,
                correlation_id=tag,
                headers=headers,
                delivery_mode=1
            )
            print(
                f"[pulsar._send_kv]info: {info}, properties: {properties}.")
            info.basic_publish(body=data, properties=properties)

    def _get_partition_send_func(self, name, tag, partitions, mq_names, mq, maximun_message_size, connection_conf: dict):
        def _fn(index, kvs):
            return self._partition_send(index, kvs, name, tag, partitions, mq_names, mq, maximun_message_size, connection_conf)

        return _fn

    def _partition_send(self, index, kvs, name, tag, partitions, mq_names, mq, maximun_message_size, connection_conf: dict):
        channel_infos = self._get_channels_index(
            index=index, mq_names=mq_names, mq=mq, connection_conf=connection_conf)

        datastream = Datastream()
        base_message_key = str(index)
        message_key_idx = 0
        count = 0

        for k, v in kvs:
            count += 1
            el = {'k': p_dumps(k).hex(), 'v': p_dumps(v).hex()}
            # roughly caculate the size of package to avoid serialization ;)
            if datastream.get_size() + sys.getsizeof(el['k']) + sys.getsizeof(el['v']) >= maximun_message_size:
                print(
                    f'[pulsar._partition_send]The size of message is: {datastream.get_size()}')
                message_key_idx += 1
                message_key = base_message_key + "_" + str(message_key_idx)
                self._send_kv(name=name, tag=tag, data=datastream.get_data(), channel_infos=channel_infos,
                              partition_size=-1, partitions=partitions, message_key=message_key)
                datastream.clear()
            datastream.append(el)

        message_key_idx += 1
        message_key = _SPLIT_.join([base_message_key, str(message_key_idx)])

        self._send_kv(name=name, tag=tag, data=datastream.get_data(), channel_infos=channel_infos,
                      partition_size=count, partitions=partitions, message_key=message_key)

        return [1]

    def _get_partition_receive_func(self, name, tag, party_id, role, party_mq_names, mq, connection_conf: dict):
        def _fn(index, kvs):
            return self._partition_receive(index, kvs, name, tag, party_id, role, party_mq_names, mq, connection_conf)

        return _fn

    def _partition_receive(self, index, kvs, name, tag, party_id, role, party_mq_names, mq, connection_conf: dict):
        queue_names = party_mq_names[index][1]
        channel_info = self._get_channel(
            mq, queue_names, party_id, role, connection_conf)

        message_key_cache = set()
        count = 0
        partition_size = -1
        all_data = []

        for method, properties, body in channel_info.consume():
            print(
                f"[pulsar._partition_receive] method: {method}, properties: {properties}.")
            if properties.message_id != name or properties.correlation_id != tag:
                # todo: fix this
                channel_info.basic_ack(delivery_tag=method.delivery_tag)
                print(
                    f"[pulsar._partition_receive]: require {name}.{tag}, got {properties.message_id}.{properties.correlation_id}")
                continue

            if properties.content_type == 'application/json':
                message_key = properties.headers["message_key"]
                if message_key in message_key_cache:
                    print(
                        f"[pulsar._partition_receive] message_key : {message_key} is duplicated")
                    channel_info.basic_ack(delivery_tag=method.delivery_tag)
                    continue

                message_key_cache.add(message_key)

                if properties.headers["partition_size"] >= 0:
                    partition_size = properties.headers["partition_size"]

                data = json.loads(body)
                data_iter = ((p_loads(bytes.fromhex(el['k'])), p_loads(
                    bytes.fromhex(el['v']))) for el in data)
                count += len(data)
                print(f"[pulsar._partition_receive] count: {count}")
                all_data.extend(data_iter)
                channel_info.basic_ack(delivery_tag=method.delivery_tag)

                if count == partition_size:
                    channel_info.cancel()
                    return all_data
            else:
                ValueError(
                    f"[pulsar._partition_receive]properties.content_type is {properties.content_type}, but must be application/json")
