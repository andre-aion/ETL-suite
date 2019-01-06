import os
import sys

module_path = os.path.abspath(os.getcwd() + '\\..')
if module_path not in sys.path:
    sys.path.append(module_path)

from scripts.storage.pythonClickhouse import PythonClickhouse
from scripts.utils import myutils
from scripts.streaming.streamingDataframe import StreamingDataframe
from config import columns, dedup_cols,columns_ch, table_dict
from tornado.gen import coroutine
from concurrent.futures import ThreadPoolExecutor

import json
import datetime

from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

import gc

from scripts.utils.mylogger import mylogger
logger = mylogger(__file__)


executor = ThreadPoolExecutor(max_workers=10)
ZOOKEEPER_SERVERS = "127.0.0.1:2181"
ZK_CHECKPOINT_PATH = '/opt/zookeeper/aion_analytics/offsets/'
ZK_CHECKPOINT_PATH = 'consumers/'


class KafkaConnectPyspark:
    pch = PythonClickhouse()
    pch.create_database('aion')
    cols = columns
    cols_ch = columns_ch

    def __init__(self, table, spark_context,conf,ssc):
        # cassandra setup

        self.table = table
        self.pch.create_table(table,table_dict, columns)


        self.zk_checkpoint_dir = ZK_CHECKPOINT_PATH
        self.spark_context = spark_context
        self.ssc = ssc
        self.conf = conf
        self.streaming_dataframe = StreamingDataframe(table, columns, dedup_cols)
        executor.submit(self.pc.create_table(table))
        
        # flags to write to which database
        self.WRITE_TO_CASSANDRA = True
        self.WRITE_TO_CLICKHOUSE = True

    @classmethod
    def set_ssc(cls, ssc):
        if 'cls.ssc' not in locals():
            cls.ssc = ssc

    def get_df(cls):
        return cls.streaming_dataframe.get_df()

    @coroutine
    def update_cassandra(cls, messages):
        cls.pc.insert_data(cls.table, messages)

    @coroutine
    def update_clickhouse(cls, messages):
        cls.pch.insert(cls.table, cls.cols_ch, messages)

    def transaction_to_tuple(cls, taken):
        messages_tuples = list() # format for saving to cass and clickhouse
        message_dask = {}
        counter = 1

        for mess in taken:
            print('block # loaded from TRANSACTION:%s', mess['block_number'])

            def munge_data():
                message_temp = {}
                for col in cls.streaming_dataframe.columns:
                    if col == 'block_timestamp':  # get time columns
                        block_timestamp = datetime.datetime.fromtimestamp(mess[col])
                        if col not in message_dask:
                            message_dask[col] = []
                        message_dask[col].append(block_timestamp)
                        message_temp[col] = block_timestamp
                        block_date = myutils.get_breakdown_from_timestamp(mess[col])
                        if 'block_date' not in message_dask:
                            message_dask['block_date'] = []
                        message_dask['block_date'].append(block_date)
                        message_temp['block_date'] = block_date

                    elif col == 'transaction_year':
                        message_temp['transaction_year'] = mess['year']

                    elif col == 'transaction_month':
                        message_temp['transaction_month'] = mess['month']

                    elif col == 'transaction_day':
                        message_temp['transaction_day'] = mess['day']

                    else:
                        if col not in ['block_date','transaction_year',
                                       'transaction_month','transaction_day']:
                            message_temp[col] = mess[col]

                message = (message_temp['transaction_hash'],message_temp['transaction_index'],
                           message_temp['block_number'],
                           message_temp['transaction_timestamp'],message_temp['block_timestamp'],
                           message_temp['block_date'],
                           message_temp['from_addr'],message_temp['to_addr'],
                           message_temp['approx_value'], message_temp['nrg_consumed'],
                           message_temp['nrg_price'], message_temp['nonce'],
                           message_temp['contract_addr'],message_temp['transaction_year'],
                           message_temp['transaction_month'], message_temp['transaction_day'])

                return message

            message_cass = munge_data()
            messages_tuples.append(message_cass)# regulate # messages in one dict
            if counter >= 10:
                #  update datastore(s)
                if cls.WRITE_TO_CASSANDRA:
                    yield cls.update_cassandra(messages_tuples)
                if cls.WRITE_TO_CLICKHOUSE:
                    yield cls.update_clickhouse(messages_tuples)
                messages_tuples = list()
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                logger.warning('tx message counter:{}'.format(counter))
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                counter = 1
            else:
                counter += 1
                del mess
                gc.collect()
        
        if cls.WRITE_TO_CASSANDRA:
            yield cls.update_cassandra(messages_tuples)
        if cls.WRITE_TO_CLICKHOUSE:
            yield cls.update_clickhouse(messages_tuples)

        del messages_tuples


    def block_to_tuple(cls, taken):
        table = 'block'
        messages_cass = list()
        message_dask = {}
        counter = 1

        for mess in taken:
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            #print(message)
            #print('message counter in taken:{}'.format(counter))
            print('block # loaded from {}:{}'.format(cls.table, mess['block_number']))

            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")


            # convert timestamp, add miner_addr
            def munge_data():
                message_temp = {}
                for col in cls.streaming_dataframe.columns:
                    if col in mess:
                        if col == 'block_timestamp':  # get time columns
                            block_timestamp = datetime.datetime.fromtimestamp(mess[col])
                            if col not in message_dask:
                                message_dask[col] = []
                            message_dask[col].append(block_timestamp)
                            message_temp[col] = block_timestamp
                            block_date = myutils.get_breakdown_from_timestamp(mess[col])
                            if 'block_date' not in message_dask:
                                message_dask['block_date'] = []
                            message_dask['block_date'].append(block_date)
                            message_temp['block_date'] = block_date


                        elif col == 'miner_address': # truncate miner address
                            if col not in message_dask:
                                message_dask[col] = []
                            message_dask[col].append(mess[col])
                            message_temp[col] = mess[col]

                            if 'miner_addr' not in message_dask:
                                message_dask['miner_addr'] = []
                            message_dask['miner_addr'].append(mess[col][0:10])
                            message_temp['miner_addr'] = mess[col][0:10]
                       
                        elif col == 'block_year':
                            message_temp['block_year'] = mess['year']

                        elif col == 'block_month':
                            message_temp['block_month'] = mess['month']

                        elif col == 'block_day':
                            message_temp['block_day'] = mess['day']

                        else:
                            if col not in message_dask:
                                message_dask[col] = []
                            message_dask[col].append(mess[col])
                            message_temp[col] = mess[col]

                message = (message_temp["block_number"], message_temp["miner_address"],
                           message_temp["miner_addr"],message_temp["nonce"], message_temp["difficulty"],
                           message_temp["total_difficulty"], message_temp["nrg_consumed"],
                           message_temp["nrg_limit"],
                           message_temp["block_size"], message_temp["block_timestamp"],
                           message_temp["block_date"], message_temp['block_year'],
                           message_temp["block_month"],message_temp['block_day'],
                           message_temp["num_transactions"],
                           message_temp["block_time"], message_temp["approx_nrg_reward"],
                           message_temp["transaction_hashes"])

                return message
                # insert to cassandra
            message_cass = munge_data()
            messages_cass.append(message_cass)

            # regulate # messages in one dict
            if counter >= 10:
                #  update streaming dataframe
                yield cls.update_cassandra(messages_cass)
                messages_cass = list()
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print('block message counter:{}'.format(counter))
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
                counter = 1
                message_dask = {}
            else:
                counter += 1
                del mess
                gc.collect()

        cls.update_cassandra(messages_cass)
        del messages_cass
        del message_dask

    def handle_rdds(cls, rdd):
        if rdd.isEmpty():
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            logger.warning('%s RDD IS EMPTY',cls.table)
            print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            return
        try:
            taken = rdd.take(5000)
            if cls.table == 'block':
                cls.block_to_tuple(taken)
            else:
                cls.transaction_to_tuple(taken)

        except Exception:
            logger.error('HANDLE RDDS:',exc_info=True)


    def get_zookeeper_instance(cls):
        from kazoo.client import KazooClient

        if 'KazooSingletonInstance' not in globals():
            globals()['KazooSingletonInstance'] = KazooClient(ZOOKEEPER_SERVERS)
            globals()['KazooSingletonInstance'].start()
        return globals()['KazooSingletonInstance']

    def read_offsets(cls, topics):
        try:
            zk = cls.get_zookeeper_instance()
            from_offsets = {}
            for topic in topics:
                logger.warning("TOPIC:%s", topic)
                #create path if it does not exist
                topic_path = ZK_CHECKPOINT_PATH + topic

                try:
                    partitions = zk.get_children(topic_path)
                    for partition in partitions:
                        topic_partition = TopicAndPartition(topic, int(partition))
                        partition_path = topic_path + '/' + partition
                        offset = int(zk.get(partition_path)[0])
                        from_offsets[topic_partition] = offset
                except Exception:
                    try:
                        topic_partition = TopicAndPartition(topic, int(0))
                        zk.ensure_path(topic_path+'/'+"0")
                        zk.set(topic_path, str(0).encode())
                        from_offsets[topic_partition] = int(0)
                        logger.warning("NO OFFSETS")
                    except Exception:
                        logger.error('MAKE FIRST OFFSET:{}', exc_info=True)

            #logger.warning("FROM_OFFSETS:%s",from_offsets)
            return from_offsets
        except Exception:
            logger.error('READ OFFSETS:%s',exc_info=True)

    def save_offsets(cls, rdd):
        try:
            zk = cls.get_zookeeper_instance()
            #logger.warning("inside save offsets:%s", zk)
            for offset in rdd.offsetRanges():
                #logger.warning("offset saved:%s",offset)
                path = ZK_CHECKPOINT_PATH + offset.topic + '/' + str(offset.partition)
                zk.ensure_path(path)
                zk.set(path, str(offset.untilOffset).encode())
        except Exception:
            logger.error('SAVE OFFSETS:%s',exc_info=True)

    def reset_partition_offset(cls, topic, partitions):
        """Delete the specified partitions within the topic that the consumer
                is subscribed to.
                :param: groupid: The consumer group ID for the consumer.
                :param: topic: Kafka topic.
                :param: partitions: List of partitions within the topic to be deleted.
        """
        try:
            for partition in partitions:
                path = "/consumers/{topic}/{partition}".format(
                    topic=topic,
                    partition=partition
                )
                zk = cls.get_zookeeper_instance()
                zk.delete(path)
                logger.warning("%s from-offsets reset",cls.table)
        except Exception:
            logger.error("delete offsets", exc_info=True)



    def kafka_stream(cls, stream):
        try:
            stream1 = stream
            logger.warning("inside kafka stream:%s", cls.table)
            stream = stream.map(lambda x: json.loads(x[1]))
            if cls.table == 'transaction':
                stream.pprint()
            stream = stream.foreachRDD(lambda rdd: cls.handle_rdds(rdd) \
                if not rdd.isEmpty() else None)

            stream1.foreachRDD(lambda rdd: cls.save_offsets(rdd))

            return stream
        except Exception:
            logger.error('KAFKA STREAM :%s', exc_info=True)


    def run(cls):
        try:
            # Get StreamingContext from checkpoint data or create a new one

            # SETUP KAFKA SOURCE
            # setup checkpointing
            topic = ['staging.aion.'+cls.table]
            partitions = list(range(0,1))
            #cls.reset_partition_offset([topic], partitions) #reset if necessary
            from_offsets = cls.read_offsets(topic)
            logger.warning("%s from_offsets in run:%s", cls.table, from_offsets)
            kafka_params = {"metadata.broker.list": "localhost:9093",
                            "auto.offset.reset": "smallest"}

            # initiate stream
            stream = KafkaUtils \
                .createDirectStream(cls.ssc, topic, kafka_params,
                                    fromOffsets=from_offsets)
            # run stream
            stream = cls.kafka_stream(stream)


            # Start the context
            cls.ssc.start()
            cls.ssc.awaitTermination()

        except Exception:
            logger.error('KAFKA/SPARK RUN:%s', exc_info=True)




