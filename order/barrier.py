#!/usr/bin/python
# -*- coding: UTF-8 -*-

import utils
import dtmimp
import logging
import redis


class AutoCursor:
    def __init__(self, cursor):
        self.cursor = cursor

    def __enter__(self):
        return self.cursor

    def __exit__(self, type, value, trace):
        self.cursor.connection.close()
        self.cursor.close()


class BranchBarrier(object):
    def __init__(self, trans_type, gid, branch_id, op):
        self.trans_type = trans_type
        self.gid = gid
        self.branch_id = branch_id
        self.op = op
        self.barrier_id = 0

    def call(self, cursor, busi_callback):
        self.barrier_id = self.barrier_id + 1
        bid = "%02d" % self.barrier_id
        try:
            orgin_branch = {
                "cancel": "try",
                "compensate": "action",
            }.get(self.op, "")
            origin_affected = insert_barrier(
                cursor,
                self.trans_type,
                self.gid,
                self.branch_id,
                orgin_branch,
                bid,
                self.op,
            )
            current_affected = insert_barrier(
                cursor, self.trans_type, self.gid, self.branch_id, self.op, bid, self.op
            )
            print(
                "origin_affected: %d, current_affected: %d"
                % (origin_affected, current_affected)
            )

            # for msg's DoAndSubmit, repeated insert should be rejected
            if self.op == "msg" and current_affected == 0:
                cursor.connection.commit()
                raise Exception("msg duplicate error")
            # origin_affected > 0 这个是空补偿; current_affected == 0 这个是重复请求或悬挂
            if (
                (self.op == "cancel" or self.op == "compensate")
                and origin_affected > 0
                or current_affected == 0
            ):
                cursor.connection.commit()
                return None
            busi_callback(cursor)
            cursor.connection.commit()
        except:
            cursor.connection.rollback()
            raise

    def query_prepared(self, cursor):
        affected_rows = insert_barrier(
            cursor, self.trans_type, self.gid, "00", "msg", "01", "rollback"
        )
        cursor.connection.commit()
        sql = (
            'select reason from dtm_barrier.barrier where gid = "%s" and branch_id = "%s" and op= "%s" and barrier_id= "%s" '
            % (self.gid, "00", "msg", "01")
        )
        cursor.execute(sql)
        data = cursor.fetchone()
        print("query_prepared_sql: %s\t query_prepared_data: %s" % (sql, data))
        if data[0] == "rollback":
            raise utils.DTMFailureError("msg rollback")

    def redis_call(self, db: redis.Redis, busi_callback):
        self.barrier_id = self.barrier_id + 1
        bid = "%02d" % self.barrier_id
        try:
            pipe = db.pipeline()
            pipe.multi()
            orgin_branch = {
                "cancel": "try",
                "compensate": "action",
            }.get(self.op, "")
            origin_affected = redis_query_barrier(
                db, self.gid, self.branch_id, orgin_branch, bid
            )
            pipe.hset(
                f"{self.gid}-{self.branch_id}-{orgin_branch}-{bid}", "op", self.op
            )
            pipe.hset(
                f"saga:{self.gid}-{self.branch_id}-{orgin_branch}-{bid}",
                "trans_type",
                self.trans_type,
            )

            current_affected = redis_query_barrier(
                db, self.gid, self.branch_id, self.op, bid
            )
            pipe.hset(f"{self.gid}-{self.branch_id}-{self.op}-{bid}", "op", self.op)
            pipe.hset(
                f"saga:{self.gid}-{self.branch_id}-{self.op}-{bid}",
                "trans_type",
                self.trans_type,
            )
            print(
                "origin_affected: %d, current_affected: %d"
                % (origin_affected, current_affected)
            )

            # for msg's DoAndSubmit, repeated insert should be rejected
            if self.op == "msg" and current_affected == 0:
                pipe.execute()
                raise Exception("msg duplicate error")
            # origin_affected > 0 这个是空补偿; current_affected == 0 这个是重复请求或悬挂
            if (
                (self.op == "cancel" or self.op == "compensate")
                and origin_affected > 0
                or current_affected == 0
            ):
                pipe.execute()
                return "empty compensation or hanging", 200
            text, code = busi_callback(db)
            pipe.execute()
            return text, code
        except:
            pipe.discard()
            raise

    # unTested
    def redis_query_prepared(self, rd: redis.Redis):
        bkey1 = (
            f"{self.gid}-{dtmimp.MsgDoBranch0}-{dtmimp.MsgDoOp}-{dtmimp.MsgDoBarrier1}"
        )
        try:
            v = rd.eval(
                """
                local v = redis.call('GET', KEYS[1])
                if v == false then
                    redis.call('SET', KEYS[1], 'rollback')
                    v = 'rollback'
                end
                if v == 'rollback' then
                    return 'FAILURE'
                end
                """,
                1,
                bkey1,
            )
            logging.debug(f"lua return v : {v}")
            if err == None:
                err = None
            if err == None and v == dtmimp.ResultFailure:
                err = dtmimp.ErrFailure
            return err
        except redis.exceptions.ResponseError as e:
            logging.debug(f"lua return err: {e}")


# return affected_rows
def insert_barrier(cursor, trans_type, gid, branch_id, op, barrier_id, reason):
    if op == "":
        return 0
    return utils.sqlexec(
        cursor,
        "insert ignore into dtm_barrier.barrier(trans_type, gid, branch_id, op, barrier_id, reason) values('%s','%s','%s','%s','%s','%s')"
        % (trans_type, gid, branch_id, op, barrier_id, reason),
    )


# redis version
def redis_query_barrier(rd: redis.Redis, gid, branch_id, op, barrier_id):
    if op == "":
        return 0
    bkey = f"{gid}-{branch_id}-{op}-{barrier_id}"
    if rd.hexists(bkey, "op"):
        current_value = rd.hget(bkey, "op")
        if current_value is not None and current_value.decode("utf-8") == op:
            return 0
    return 1
