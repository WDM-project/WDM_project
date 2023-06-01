#!/usr/bin/python
# -*- coding: UTF-8 -*-

import requests
import utils
import requests
import logging
import threading
import json


# Errors
class ErrFailure(Exception):
    pass


class ErrOngoing(Exception):
    pass


class ErrDuplicated(Exception):
    pass


# Constants
MapSuccess = {"dtm_result": "SUCCESS"}
MapFailure = {"dtm_result": "FAILURE"}
BarrierTableName = "dtm_barrier.barrier"

# Store resty clients
resty_clients = {}


class SessionWithTimeout(requests.Session):
    def __init__(self, timeout=None):
        super().__init__()
        self.timeout = timeout

    def request(self, *args, **kwargs):
        if self.timeout is not None:
            kwargs.setdefault("timeout", self.timeout)
        return super().request(*args, **kwargs)


def get_resty_client(timeout=None):
    global resty_clients
    client = resty_clients.get(timeout)
    if client is None:
        client = SessionWithTimeout(timeout)
        resty_clients[timeout] = client
    return client


def log_before_request(method, url, **kwargs):
    logging.debug(f"requesting: {method} {url} {json.dumps(kwargs)}")


def log_after_response(response):
    logging.debug(f"requested: {response.status_code} {response.url} {response.text}")


# // ResultFailure for result of a trans/trans branch
# // Same as HTTP status 409 and GRPC code 10
ResultFailure = "FAILURE"
# // ResultSuccess for result of a trans/trans branch
# // Same as HTTP status 200 and GRPC code 0
ResultSuccess = "SUCCESS"
# // ResultOngoing for result of a trans/trans branch
# // Same as HTTP status 425 and GRPC code 9
ResultOngoing = "ONGOING"

# // OpTry branch type for TCC
OpTry = "try"
# // OpConfirm branch type for TCC
OpConfirm = "confirm"
# // OpCancel branch type for TCC
OpCancel = "cancel"
# // OpAction branch type for message, SAGA, XA
OpAction = "action"
# // OpCompensate branch type for SAGA
OpCompensate = "compensate"
# // OpCommit branch type for XA
OpCommit = "commit"
# // OpRollback branch type for XA
OpRollback = "rollback"

# // DBTypeMysql const for driver mysql
DBTypeMysql = "mysql"
# // DBTypePostgres const for driver postgres
DBTypePostgres = "postgres"
# // DBTypeRedis const for driver redis
DBTypeRedis = "redis"
# // Jrpc const for json-rpc
Jrpc = "json-rpc"
# // JrpcCodeFailure const for json-rpc failure
JrpcCodeFailure = -32901

# // JrpcCodeOngoing const for json-rpc ongoing
JrpcCodeOngoing = -32902

# // MsgDoBranch0 const for DoAndSubmit barrier branch
MsgDoBranch0 = "00"
# // MsgDoBarrier1 const for DoAndSubmit barrier barrierID
MsgDoBarrier1 = "01"
# // MsgDoOp const for DoAndSubmit barrier op
MsgDoOp = "msg"
# //MsgTopicPrefix const for Add topic msg
MsgTopicPrefix = "topic://"

# // XaBarrier1 const for xa barrier id
XaBarrier1 = "01"

# // ProtocolGRPC const for protocol grpc
ProtocolGRPC = "grpc"
# // ProtocolHTTP const for protocol http
ProtocolHTTP = "http"


def trans_call_dtm(dtm, body, operation, request_timeout):
    url = "%s/%s" % (dtm, operation)
    r = requests.post(url, json=body, timeout=request_timeout)
    utils.check_result(r)
    return body
