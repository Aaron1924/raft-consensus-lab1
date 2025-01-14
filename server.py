import math
import multiprocessing
import random
import sys
import threading
import time
from concurrent import futures
from typing import Callable, Any
from pymongo import MongoClient
import grpc
import raft_pb2
import raft_pb2_grpc as pb_grpc

# noinspection DuplicatedCode
AppendRequest = raft_pb2.AppendRequest
AppendResponse = raft_pb2.AppendResponse
GetLeaderResponse = raft_pb2.GetLeaderResponse
SuspendRequest = raft_pb2.SuspendRequest
Void = raft_pb2.Void
VoteRequest = raft_pb2.VoteRequest
VoteResponse = raft_pb2.VoteResponse
Key = raft_pb2.Key
KeyValue = raft_pb2.KeyValue
SetValResponse = raft_pb2.SetValResponse
GetValResponse = raft_pb2.GetValResponse
LogEntry = raft_pb2.LogEntry

PartitionRequest = raft_pb2.PartitionRequest
PartitionResponse = raft_pb2.PartitionResponse

HEARTBEAT_INTERVAL = 200  # ms
ELECTION_INTERVAL = 500, 800  # ms


def parse_server_config(config: str) -> (int, str):
    params = config.replace('[', '').replace(']', '').split()
    return int(params[0].replace('Node', '')), f"{params[1]}:{params[2]}"

def generate_random_timeout() -> int:
    return random.randint(ELECTION_INTERVAL[0], ELECTION_INTERVAL[1])


def start_after_time(period_sec: float, func: Callable, *args, **kwargs) -> None:
    timer = threading.Timer(period_sec, func, args, kwargs)
    timer.start()


# noinspection PyUnresolvedReferences
class RepeatTimer(threading.Timer):

    def run(self) -> None:
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


def get_service_stub(node_address: str) -> pb_grpc.RaftElectionServiceStub:
    channel = grpc.insecure_channel(node_address)
    client_stub = pb_grpc.RaftElectionServiceStub(channel)
    return client_stub


class RaftElectionService(pb_grpc.RaftElectionServiceServicer):

    def __init__(self, server_id: int, server_address: str, servers: dict[int, str]) -> None:
        super().__init__()
        
        #MongoBD connection
        database_name = f"raft_node_{self.server_id}_db"

        #Change URI to your MongoDB server
        self.mongo_client = MongoClient("mongodb://127.0.0.1:27017/")
        self.db = self.mongo_client[database_name]
        self.term_collection = self.db["current terms"]
        self.logs_collection = self.db["logs"]
        self.status_collection = self.db["node_status"]
        
        print(f"MongoDB connected. DB: {self.db}, Term Collection: {self.term_collection}, Logs Collection: {self.logs_collection}")
        committed_data = self.db["committed_data"].find()
        for item in committed_data:
            self.data[item["key"]] = item["value"]  
        print(f"Loaded committed data from MongoDB: {self.data}")
        # log receiving
        self.logs: [LogEntry] = []        
        self.commit_length: int = 0
        self.sent_length: dict[str, int] = {}
        self.acked_length: dict[str, int] = {}
        self.data: dict[str, str] = {}

        self.current_term = 0
        self.server_id = server_id
        self.server_address = server_address
        self.servers = servers
        self.election_timeout = generate_random_timeout()
        self.state = None
        self.election_timer = None
        self.current_vote = None
        self.start_following()
        self.leader_id = None
        self.leader_address = None
        self.leader_timer = None
        self.partition1 = []
        self.partition2 = []
        self.is_partitioned = False
        self.should_interrupt = False


        #Load Current Term or set to 0
        term_data = self.term_collection.find_one({"server_id": server_id})
        if term_data:
            self.current_term = term_data["current_term"]
        else:
            self.current_term = 0
            self.term_collection.insert_one({"server_id": server_id, "current_term": self.current_term})

        #Save initial status as "follower"
        self.status_collection.update_one(
            {"server.py": self.server_id},
            {"$set":{"status": "follower", "term": self.current_term}},
            upsert=True
        )
        self.start_following()




    def start_election_timer(self) -> threading.Timer:
        """ Unit of timeout is ms """
        election_timer = threading.Timer(self.election_timeout / 1000, self.start_election)
        election_timer.start()
        return election_timer

    def start_following(self):
        self.state = "follower"
        print(f"I am a follower. Term: {self.current_term}")

        #Save current term to MongoDB
        self.term_collection.update_one(
            {"server_id": self.server_id},
            {"$set": {"current_term": self.current_term}},
            upsert=True
        )
        self.status_collection.update_one(
            {"server_id" : self.server_id},
            {"$set": {"status": "follower", "term": self.current_term}},
            upsert=True
        )
        self.election_timer = self.start_election_timer()

    def start_election(self):
        print("The leader is dead")
        print(f"I am a candidate. Term: {self.current_term}")
        if self.state != "follower":
            return

        self.state = "candidate"
        self.current_term += 1

        #Save current term to MongoDB
        self.term_collection.update_one(
            {"server_id": self.server_id},
            {"$set": {"current_term": self.current_term}},
            upsert=True
        )

        self.status_collection.update_one(
        {"server_id": self.server_id},
        {"$set": {"status": "candidate", "term": self.current_term}},
        upsert=True
        )

        self.current_vote = self.server_id
        print(f"Voted for node {self.server_id}")
        number_of_voted = 1  # because server initially votes for itself

        queue = multiprocessing.Queue()
        threads = []

        last_term = 0
        if len(self.logs) > 0:
            last_term = self.logs[-1].term

        for _, server_address in self.servers.items():
            thread = threading.Thread(target=self.request_election_vote, args=(server_address,
                                                                               queue,
                                                                               last_term,
                                                                               len(self.logs)))
            thread.daemon = True
            threads.append(thread)
            if self.state != "candidate":
                return
            thread.start()

        for thread in threads:
            thread.join()

        print(f"Votes received", end=": ")
        while not queue.empty():
            vote_result = queue.get()
            if vote_result is None:  # if server is not responding
                continue

            if vote_result.term > self.current_term:
                self.current_term = vote_result.term
                self.current_vote = None
                self.start_following()
                return
            if vote_result.result:
                number_of_voted += 1
        print(number_of_voted)
        if number_of_voted > (len(self.servers) + 1) / 2:
            self.start_leading()
        else:
            self.election_timeout = generate_random_timeout()
            self.start_following()

    def request_election_vote(self, address: str, queue, last_term: int, last_index: int) -> None:
        client_stub = get_service_stub(node_address=address)
        # noinspection PyBroadException
        try:
            result: VoteResponse = client_stub.RequestVote(
                VoteRequest(candidateTerm=self.current_term,
                            candidateId=self.server_id,
                            lastLogTerm=last_term,
                            lastLogIndex=last_index))
            queue.put(result)
        except Exception:
            pass

    def start_leading(self):
        print(f"I am a leader. Term: {self.current_term}")
        self.election_timer.cancel()
        self.state = "leader"
        self.leader_id = self.server_id
        self.leader_address = self.server_address

        # Save current status to MongoDB
        self.status_collection.update_one(
        {"server_id": self.server_id},
        {"$set": {"status": "leader", "term": self.current_term}},
        upsert=True
        )

        for _, server_address in self.servers.items():
            self.sent_length[server_address] = len(self.logs)
            self.acked_length[server_address] = 0

        self.leader_timer = RepeatTimer(HEARTBEAT_INTERVAL / 1000, function=self.send_heartbeats)
        self.leader_timer.start()
        while self.state == "leader":
            if self.should_interrupt:
                self.leader_timer.cancel()
                return
        print(f"I am a follower. Term: {self.current_term}")
        self.leader_timer.cancel()

    def send_heartbeats(self):
        threads = []

        for _, server_address in self.servers.items():
            # replicate log
            thread = threading.Thread(target=self.send_heartbeat, args=(server_address,))
            thread.daemon = True
            threads.append(thread)
            thread.start()
            # replicate log end

        for thread in threads:
            if self.should_interrupt:
                return
            thread.join()

    def send_heartbeat(self, server_address):
        client_stub = get_service_stub(server_address)

        if self.sent_length and self.logs:
            # if for current moment there are new logs => send them with heartbeat
            i = self.sent_length[server_address]
            entries: [LogEntry] = self.logs[i:]
            prev_log_term: int = self.logs[i - 1].term if (i > 0) else 0
            # end
        else:
            i = 0
            prev_log_term = 0
            entries = []

        # noinspection PyBroadException
        try:
            result = client_stub.AppendEntries(
                AppendRequest(leaderTerm=self.current_term,
                              leaderId=self.server_id,
                              prevLogIndex=i,
                              prevLogTerm=prev_log_term,
                              entries=entries,
                              leaderCommitIndex=self.commit_length))
            if not result.success:
                self.current_term = result.term
                self.state = "follower"
                return

            # print(f"Heartbeat with append entries was sent to {server_address}")

            self.on_append_response(follower_address=server_address,
                                    term=result.term,
                                    ack=result.ack,
                                    success=result.success)

        except Exception:
            pass

    def __acks(self, length: int):
        acks = 1  # leader acks its changes
        for _, address in self.servers.items():
            if self.acked_length[address] >= length:
                acks += 1
        return acks

    def commit_log_entries(self):
        min_acks = int(math.ceil((len(self.servers) + 1) / 2))
        ready = []
        for length in range(1, len(self.logs) + 1):
            if self.__acks(length) >= min_acks:
                ready.append(length)
        if len(ready) > 0 and max(ready) > self.commit_length and self.logs[max(ready) - 1].term == self.current_term:
            for i in range(self.commit_length, max(ready)):
                key_value = self.logs[i].keyValue
                # Update MongoDB with the committed key-value pair
                self.db["committed_data"].update_one(
                    {"key": key_value.key}, 
                    {"$set": {"value": key_value.value, "term": self.current_term}},
                    upsert=True # Insert if the key doesn't exist
                )
                self.data[key_value.key] = key_value.value  # Also update in-memory cache

            self.commit_length = max(ready)

    def on_append_response(self, follower_address: str, term: int, ack: int, success: bool):
        if term == self.current_term and self.state == "leader":
            if success:
                self.sent_length[follower_address] = ack
                self.acked_length[follower_address] = ack
                self.commit_log_entries()
            elif self.sent_length[follower_address] > 0:
                self.sent_length[follower_address] = self.sent_length[follower_address] - 1
                self.send_heartbeat(follower_address)
        elif term > self.current_term:
            self.current_term = term
            if self.state != "follower":
                self.start_following()
            self.current_vote = None
#new
    def append_logs(self, log_length: int, leader_commit: int, entries: [LogEntry]) -> None:
        if len(entries) > 0 and len(self.logs) > log_length:
            if self.logs[log_length].term != entries[0].term:
                self.logs = self.logs[:log_length-1]

        if log_length + len(entries) > len(self.logs):
            for i in range(len(self.logs) - log_length, len(entries)):
                self.logs.append(entries[i])

                #Save to MongoDB
                self.logs_collection.insert_one(
                    {"server_id": self.server_id,
                    "term": entries[i].term,
                    "key": entries[i].keyValue.key,
                    "value": entries[i].keyValue.value                    
                    }
                )

        if leader_commit > self.commit_length:
            for i in range(self.commit_length, leader_commit):
                key_value: KeyValue = self.logs[i].keyValue
                self.data[key_value.key] = key_value.value  # deliver
            self.commit_length = leader_commit
    
    def Partition(self, request: PartitionRequest, context):
        """ Partition the cluster into two sub-clusters """
        self.partition1 = request.partition1
        self.partition2 = request.partition2
        self.is_partitioned = True
        
        # Update internal status (perhaps disable communication between partitions here)
        print(f"Partitioning into two sub-clusters: {self.partition1} and {self.partition2}")
        
        return PartitionResponse(success=True)
    
    # Implement Unpartition RPC method
    def Unpartition(self, request, context):
        """ Unpartition the cluster, allowing full communication """
        self.partition1 = []
        self.partition2 = []
        self.is_partitioned = False
        
        # Allow full communication again
        print("Cluster unpartitioned. Full communication restored.")
        
        return PartitionResponse(success=True)

    def RequestVote(self, request, context):
        candidate_term = request.candidateTerm
        candidate_id = request.candidateId
        last_log_index = request.lastLogIndex
        last_log_term = request.lastLogTerm

        if not self.logs:
            log_ok = True
        else:
            my_log_term = self.logs[-1].term
            log_ok = (last_log_term > my_log_term) or \
                     (last_log_term == my_log_term and last_log_index >= len(self.logs))

        voted_ok = self.current_vote is None or self.current_vote == candidate_id
        term_ok = candidate_term > self.current_term or (candidate_term == self.current_term and voted_ok)

        if log_ok and term_ok:
            self.current_term = candidate_term
            self.current_vote = candidate_id
            if self.state != "follower":
                self.start_following()
            print(f"Voted for: {candidate_id}")
            return VoteResponse(term=self.current_term, result=True)
        else:
            return VoteResponse(term=self.current_term, result=False)

    def AppendEntries(self, request: AppendRequest, context):
        entries = request.entries
        str_entries = []
        for entry in entries:
            str_entries.append((entry.keyValue.key, entry.keyValue.value))

        leader_id = request.leaderId
        term = request.leaderTerm
        log_length = request.prevLogIndex
        log_term = request.prevLogTerm
        leader_commit = request.leaderCommitIndex
        entries: [LogEntry] = request.entries

        self.leader_id = leader_id
        self.leader_address = self.servers[leader_id]
        try:
            if self.state == "follower":
                self.election_timer.cancel()
                self.election_timer = self.start_election_timer()

            if term > self.current_term:
                self.current_term = term
                self.current_vote = None

            if not self.logs:
                log_ok = True
            else:
                log_ok = len(self.logs) >= log_length
                if log_ok and log_length > 0:
                    log_ok = log_term == self.logs[-1].term

            if term == self.current_term and log_ok:
                self.append_logs(log_length, leader_commit, entries)
                ack = log_length + len(entries)
                return AppendResponse(term=self.current_term, success=True, ack=ack)
            else:
                return AppendResponse(term=self.current_term, success=False, ack=0)
        except grpc.RpcError as e:
            print(str(e))

    def GetLeader(self, request, context):
        print("Command from client: getleader")
        node_id, node_address = None, None

        if self.state == "candidate":
            if not self.current_vote:
                node_id, node_address = self.current_vote, self.servers[self.current_vote]
        else:
            node_id, node_address = self.leader_id, self.leader_address

        print(f"{node_id} {node_address}")
        return GetLeaderResponse(nodeId=node_id, nodeAddress=node_address)

    def SetVal(self, request: KeyValue, context):
        if self.state == "leader":
            log_entry = LogEntry(keyValue=request, term=self.current_term)
            self.logs.append(log_entry)
            self.acked_length[self.server_address] = len(self.logs)

            self.logs_collection.insert_one(
                {"server_id": self.server_id,
                "term": log_entry.term,
                "key": log_entry.keyValue.key,
                "value": log_entry.keyValue.value
                }
            )
            return SetValResponse(success=True)
        else:
            client_stub = get_service_stub(self.leader_address)
            return client_stub.SetVal(request)

    def GetVal(self, request, context):
        log_entry = self.logs_collection.find_one(
            {"server_id": self.server_id, "key": request.key}
        )
        if log_entry:
            value = log_entry["value"]
            return GetValResponse(success=True, value=value)
        else:
            val = self.db["committed_data"].find_one({"key":request.key})
            if val:
                return GetValResponse(success=True, value=val["value"])
            else:  
                return GetValResponse(success=False)

    def Suspend(self, request, context):
        pass


class SuspendableRaftElectionService(RaftElectionService):

    def __init__(self, server_id: int, server_address: str, servers: dict[int, str]) -> None:
        # Ensure that the RaftElectionService is properly initialized
        self.server_id = server_id  
        self.server_address = server_address
        self.servers = servers
        self.data = {}
        super().__init__(server_id, server_address, servers)
        self.suspended = False

    def RequestVote(self, request, context):
        return self.__wrap_with_suspend(super().RequestVote, request, context)

    def AppendEntries(self, request: AppendRequest, context):
        return self.__wrap_with_suspend(super().AppendEntries, request, context)

    def GetLeader(self, request, context):
        return self.__wrap_with_suspend(super().GetLeader, request, context)

    def SetVal(self, request, context):
        return self.__wrap_with_suspend(super().SetVal, request, context)

    def GetVal(self, request, context):
        return self.__wrap_with_suspend(super().GetVal, request, context)

    def Suspend(self, request, context):
        return self.__wrap_with_suspend(self.__suspend, request, context)

    # noinspection PyUnusedLocal
    def __suspend(self, request, context) -> Void:
        period = request.period
        print(f"Command from client: suspend {request.period}")

        was_follower = self.state == "follower"

        self.suspended = True

        # actual suspend only after function return
        start_after_time(period_sec=0.025, func=self.__make_suspend, period=period, was_follower=was_follower)

        return Void()

    def __make_suspend(self, period: int, was_follower: bool) -> None:
        self.state = "follower"
        self.election_timer.cancel()
        if self.leader_timer is not None:
            self.leader_timer.cancel()
        print(f"Sleeping for {period} seconds")
        time.sleep(period)

        # after wake up
        if was_follower:
            print(f"I am a follower. Term: {self.current_term}")
        self.election_timer = self.start_election_timer()

        self.suspended = False

    def __wrap_with_suspend(self, func: Callable, request, context) -> Any:
        if self.suspended:
            msg = "Server is suspended"
            context.set_details(msg)
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            return Void()
        else:
            return func(request, context)


def configure_server(server_addr: str, server_id: int, other_servers_addresses: [str]):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    server.add_insecure_port(server_addr)
    print(f"The server starts at {server_addr}")
    service = SuspendableRaftElectionService(server_id, server_addr, other_servers_addresses)
    pb_grpc.add_RaftElectionServiceServicer_to_server(
        service,
        server
    )
    return service, server


def start_server() -> None:
    needed_server_id = int(sys.argv[1])
    config_file = open("config.conf", "r")
    servers = {}
    needed_server_address = None
    for server_line in config_file.read().splitlines():
        server_id, address = parse_server_config(server_line)
        if server_id == needed_server_id:
            needed_server_address = address
            continue
        servers[server_id] = address
    config_file.close()

    if needed_server_address is None:
        print("No such available server id")
        sys.exit()

    service, server = configure_server(server_addr=needed_server_address,
                                       server_id=needed_server_id,
                                       other_servers_addresses=servers)

    try:
        server.start()
        server.wait_for_termination()
    except KeyboardInterrupt:
        service.should_interrupt = True
        server.stop(grace=None)
        print("Shutting down")
        sys.exit(0)


if __name__ == "__main__":
    start_server()