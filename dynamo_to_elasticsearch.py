from __future__ import print_function
import ConfigParser
import os
import logging

import re
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
# Process DynamoDB Stream records and insert the object in ElasticSearch
# Use the Table name as index and doc_type name
# Force index refresh upon all actions for close to realtime reindexing
# Use IAM Role for authentication
# Properly unmarshal DynamoDB JSON types. Binary NOT tested.
TABLE_PREFIX = 'sfdc'



class Elasticsearchservice(object):
    def __init__(self, es):
        self.es = es
        logging.info(es.info())

    def __ensure_index(self, index_name):
        if self.es.indices.exists(index_name) == False:
            logging.info("Create missing index: %s", index_name)

            self.es.indices.create(
                index_name,
                body='{"settings": { "index.mapping.coerce": true } }')

            logging.info("Index created: %s", index_name)

    def insert(self, record):
        ddb_table, es_table = self.getTable(record)
        logging.info("Source Table: %s", ddb_table)
        # Create index if missing
        self.__ensure_index(es_table)
        # Unmarshal the DynamoDB JSON to a normal JSON
        doc = json.dumps(self.unmarshalJson(record))

        logging.info("New document to Index:")
        logging.info(doc)

        newId = self.generateId(record)
        self.es.index(index=es_table, body=doc, id=newId, doc_type=es_table,
                      refresh=True)
        logging.info("Success - New Index ID: %s", newId)

    # Process MODIFY events
    def modify_document(self, record):
        ddb_table, es_table = self.getTable(record)
        logging.info("Source Table: %s", ddb_table)

        docId = self.generateId(record)
        logging.info("KEY: %s", docId)

        # Unmarshal the DynamoDB JSON to a normal JSON
        doc = json.dumps(self.unmarshalJson(record))

        logging.info("Updated document: %s", doc)

        # We reindex the whole document as ES accepts partial docs
        self.es.index(index=es_table, body=doc, id=docId, doc_type=es_table,
                      refresh=True)

        logging.info("Success - Updated index ID: %s", docId)

    # Process REMOVE events
    def remove_document(self, record):
        ddb_table, es_table = self.getTable(record)
        logging.info("Source Table: %s", ddb_table)

        docId = self.generateId(record)
        logging.info("Deleting document ID: %s", docId)

        self.es.delete(
            index=es_table, id=docId, doc_type=es_table, refresh=True)
        logging.info("Successly removed")

    def getTable(self, record):
        pass

    def generateId(self, record):
        pass

    def unmarshalJson(self, node):
        pass
    def unmarshalValue(self, node):
        pass

class ElasticsearchserviceDynamo(object):
    def __init__(self, es):
        super(ElasticsearchserviceDynamo, self).__init__(es)

    def getTable(self, record):
        p = re.compile('arn:aws:dynamodb:.*?:.*?:table/([0-9a-zA-Z_-]+)/.+')
        m = p.match(record['eventSourceARN'])
        if m is None:
            raise Exception("Table not found in SourceARN")
        real_table = m.group(1).lower()
        return real_table, '{0}-{1}'.format(TABLE_PREFIX, real_table)

    # Generate the ID for ES. Used for deleting or updating item later


    def generateId(self, record):
        keys = self.unmarshalJson(record['dynamodb']['Keys'])

        # Concat HASH and RANGE key with | in between
        newId = ""
        i = 0
        for key, value in keys.items():
            if (i > 0):
                newId += "|"
            newId += str(value)
            i += 1

        return newId

    # Unmarshal a JSON that is DynamoDB formatted


    def unmarshalJson(self, node):
        data = {}
        data["M"] = node
        return self.unmarshalValue(data, True)

    # ForceNum will force float or Integer to


    def unmarshalValue(self, node, forceNum=False):
        for key, value in node.items():
            if (key == "NULL"):
                return None
            if (key == "S" or key == "BOOL"):
                return value
            if (key == "N"):
                if (forceNum):
                    return int_or_float(value)
                return value
            if (key == "M"):
                data = {}
                for key1, value1 in value.items():
                    data[key1] = unmarshalValue(value1, True)
                return data
            if (key == "SS" or key == "BS" or key == "L"):
                data = []
                for item in value:
                    data.append(unmarshalValue(item))
                return data
            if (key == "NS"):
                data = []
                for item in value:
                    if (forceNum):
                        data.append(int_or_float(item))
                    else:
                        data.append(item)
                return data


def get_config(section,
               config_path=os.path.join('dynamo_to_elasticsearch.cfg')):
    """Retrieve config section from file"""
    config = ConfigParser.RawConfigParser()
    logging.info('config_path: %s', config_path)
    config.read(config_path)
    if config.has_section(section):
        return dict(config.items(section))


def lambda_handler(event, context):
    config = get_config('dynamo_to_es')
    session = boto3.session.Session()
    credentials = session.get_credentials()

    # Get proper credentials for ES auth
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key,
                       session.region_name, 'es',
                       session_token=credentials.token)

    # Connect to ES
    es = Elasticsearch(
        config['es_endpoint'], http_auth=awsauth, use_ssl=True,
        verify_certs=True, connection_class=RequestsHttpConnection)
    ess = ElasticsearchserviceDynamo(es)

    # Loop over the DynamoDB Stream records
    for record in event['Records']:

        logging.info("New Record to process: %s", json.dumps(record))

        try:

            if record['eventName'] == 'INSERT':
                ess.insert(record)
            elif record['eventName'] == 'REMOVE':
                ess.remove(record)
            elif record['eventName'] == 'MODIFY':
                ess.modify(record)

        except Exception as error:
            logging.error(error)
            continue

# Return the dynamoDB table that received the event. Lower case it


# Detect number type and return the correct one
def int_or_float(s):
    try:
        return int(s)
    except ValueError:
        return float(s)
