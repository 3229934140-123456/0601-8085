import threading
import time
import random
from enum import Enum
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass

from .log import LogEntry, LogStore
from .state_machine import StateMachine
from .transport import Transport, RaftMessage


class RaftState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class ApplyResult:
    success: bool
    result: Any = None
    error: str = None


class RaftNode:
    def __init__(
        self,
        node_id: str,
        peers: List[str],
        transport: Transport,
        state_machine: StateMachine,
        election_timeout_min: float = 0.15,
        election_timeout_max: float = 0.30,
        heartbeat_interval: float = 0.05,
    ):
        self.node_id = node_id
        self.peers = peers
        self.transport = transport
        self.state_machine = state_machine

        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval

        self._lock = threading.RLock()
        self._state = RaftState.FOLLOWER
        self._current_term = 0
        self._voted_for: Optional[str] = None
        self._log = LogStore()

        self._commit_index = -1
        self._last_applied = -1

        self._next_index: Dict[str, int] = {}
        self._match_index: Dict[str, int] = {}

        self._last_heartbeat = time.time()
        self._election_timeout = self._random_election_timeout()
        self._last_heartbeat_sent = 0.0

        self._votes_received = set()

        self._running = False
        self._main_thread = None
        self._msg_thread = None

        self._apply_callbacks: Dict[int, Callable] = {}
        self._apply_lock = threading.Lock()

        self._msg_queue: List[RaftMessage] = []
        self._msg_queue_lock = threading.Lock()

        self.transport.register_node(node_id)

    def _random_election_timeout(self) -> float:
        return random.uniform(self.election_timeout_min, self.election_timeout_max)

    def start(self):
        self._running = True
        self._last_heartbeat = time.time()

        self._msg_thread = threading.Thread(target=self._msg_receiver_loop, daemon=True)
        self._msg_thread.start()

        self._main_thread = threading.Thread(target=self._main_loop, daemon=True)
        self._main_thread.start()

    def stop(self):
        self._running = False
        if self._main_thread:
            self._main_thread.join(timeout=1.0)
        if self._msg_thread:
            self._msg_thread.join(timeout=1.0)

    def _msg_receiver_loop(self):
        while self._running:
            msg = self.transport.recv(self.node_id, timeout=0.001)
            if msg:
                with self._msg_queue_lock:
                    self._msg_queue.append(msg)

    def _drain_messages(self) -> List[RaftMessage]:
        with self._msg_queue_lock:
            msgs = self._msg_queue
            self._msg_queue = []
            return msgs

    def _main_loop(self):
        while self._running:
            with self._lock:
                state = self._state

            msgs = self._drain_messages()
            if msgs:
                with self._lock:
                    for msg in msgs:
                        self._handle_message(msg)

            with self._lock:
                self._maybe_apply_committed()

            if state == RaftState.FOLLOWER:
                self._tick_follower()
            elif state == RaftState.CANDIDATE:
                self._tick_candidate()
            elif state == RaftState.LEADER:
                self._tick_leader()

            time.sleep(0.005)

    def _tick_follower(self):
        with self._lock:
            if self._state != RaftState.FOLLOWER:
                return
            elapsed = time.time() - self._last_heartbeat
            if elapsed >= self._election_timeout:
                self._start_election()

    def _tick_candidate(self):
        with self._lock:
            if self._state != RaftState.CANDIDATE:
                return
            elapsed = time.time() - self._last_heartbeat
            if elapsed >= self._election_timeout:
                self._start_election()

    def _tick_leader(self):
        now = time.time()
        with self._lock:
            if self._state != RaftState.LEADER:
                return
            if now - self._last_heartbeat_sent >= self.heartbeat_interval:
                self._send_heartbeats()
                self._last_heartbeat_sent = now

    def _start_election(self):
        self._state = RaftState.CANDIDATE
        self._current_term += 1
        self._voted_for = self.node_id
        self._votes_received = {self.node_id}
        self._election_timeout = self._random_election_timeout()
        self._last_heartbeat = time.time()

        last_log_index = self._log.last_index()
        last_log_term = self._log.last_term()

        for peer in self.peers:
            self.transport.send(RaftMessage(
                src=self.node_id,
                dst=peer,
                msg_type="RequestVote",
                payload={
                    "term": self._current_term,
                    "candidate_id": self.node_id,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                }
            ))

        total_nodes = len(self.peers) + 1
        if len(self._votes_received) > total_nodes // 2:
            self._become_leader()

    def _send_heartbeats(self):
        if self._state != RaftState.LEADER:
            return
        for peer in self.peers:
            self._send_append_entries(peer)

    def _send_append_entries(self, peer: str):
        next_idx = self._next_index.get(peer, 0)
        prev_log_index = next_idx - 1
        prev_log_term = self._log.term_at(prev_log_index)

        entries = self._log.entries_from(next_idx)

        self.transport.send(RaftMessage(
            src=self.node_id,
            dst=peer,
            msg_type="AppendEntries",
            payload={
                "term": self._current_term,
                "leader_id": self.node_id,
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": [e.__dict__ for e in entries],
                "leader_commit": self._commit_index,
            }
        ))

    def _handle_message(self, msg: RaftMessage):
        msg_type = msg.msg_type
        payload = msg.payload

        if msg_type == "RequestVote":
            self._handle_request_vote(msg.src, payload)
        elif msg_type == "RequestVoteResponse":
            self._handle_request_vote_response(msg.src, payload)
        elif msg_type == "AppendEntries":
            self._handle_append_entries(msg.src, payload)
        elif msg_type == "AppendEntriesResponse":
            self._handle_append_entries_response(msg.src, payload)

    def _handle_request_vote(self, src: str, payload: Dict):
        term = payload["term"]
        candidate_id = payload["candidate_id"]
        last_log_index = payload["last_log_index"]
        last_log_term = payload["last_log_term"]

        if term > self._current_term:
            self._current_term = term
            self._state = RaftState.FOLLOWER
            self._voted_for = None
            self._last_heartbeat = time.time()

        if term < self._current_term:
            self.transport.send(RaftMessage(
                src=self.node_id,
                dst=src,
                msg_type="RequestVoteResponse",
                payload={
                    "term": self._current_term,
                    "vote_granted": False,
                }
            ))
            return

        can_vote = self._voted_for is None or self._voted_for == candidate_id

        log_ok = (last_log_term > self._log.last_term() or
                  (last_log_term == self._log.last_term() and
                   last_log_index >= self._log.last_index()))

        vote_granted = can_vote and log_ok

        if vote_granted:
            self._voted_for = candidate_id
            self._last_heartbeat = time.time()

        self.transport.send(RaftMessage(
            src=self.node_id,
            dst=src,
            msg_type="RequestVoteResponse",
            payload={
                "term": self._current_term,
                "vote_granted": vote_granted,
            }
        ))

    def _handle_request_vote_response(self, src: str, payload: Dict):
        term = payload["term"]
        vote_granted = payload["vote_granted"]

        if term > self._current_term:
            self._current_term = term
            self._state = RaftState.FOLLOWER
            self._voted_for = None
            self._last_heartbeat = time.time()
            return

        if self._state != RaftState.CANDIDATE or term != self._current_term:
            return

        if vote_granted:
            self._votes_received.add(src)
            if len(self._votes_received) > (len(self.peers) + 1) // 2:
                self._become_leader()

    def _become_leader(self):
        self._state = RaftState.LEADER
        last_idx = self._log.last_index()
        for peer in self.peers:
            self._next_index[peer] = last_idx + 1
            self._match_index[peer] = -1
        self._last_heartbeat_sent = 0.0

    def _handle_append_entries(self, src: str, payload: Dict):
        term = payload["term"]
        leader_id = payload["leader_id"]
        prev_log_index = payload["prev_log_index"]
        prev_log_term = payload["prev_log_term"]
        entries_data = payload["entries"]
        leader_commit = payload["leader_commit"]

        if term > self._current_term:
            self._current_term = term
            self._voted_for = None
            self._state = RaftState.FOLLOWER

        if term < self._current_term:
            self.transport.send(RaftMessage(
                src=self.node_id,
                dst=src,
                msg_type="AppendEntriesResponse",
                payload={
                    "term": self._current_term,
                    "success": False,
                    "match_index": -1,
                }
            ))
            return

        self._state = RaftState.FOLLOWER
        self._last_heartbeat = time.time()
        self._election_timeout = self._random_election_timeout()

        log_ok = (prev_log_index == -1 or
                  (prev_log_index <= self._log.last_index() and
                   self._log.term_at(prev_log_index) == prev_log_term))

        if not log_ok:
            self.transport.send(RaftMessage(
                src=self.node_id,
                dst=src,
                msg_type="AppendEntriesResponse",
                payload={
                    "term": self._current_term,
                    "success": False,
                    "match_index": -1,
                }
            ))
            return

        entries = [LogEntry(**e) for e in entries_data]

        if entries:
            new_entries_start = 0
            for i, entry in enumerate(entries):
                existing_index = entry.index
                if existing_index <= self._log.last_index():
                    if self._log.term_at(existing_index) != entry.term:
                        self._log.truncate_from(existing_index)
                        new_entries_start = i
                        break
                else:
                    new_entries_start = i
                    break
            else:
                new_entries_start = len(entries)

            if new_entries_start < len(entries):
                self._log.append(entries[new_entries_start:])

        if leader_commit > self._commit_index:
            self._commit_index = min(leader_commit, self._log.last_index())

        self.transport.send(RaftMessage(
            src=self.node_id,
            dst=src,
            msg_type="AppendEntriesResponse",
            payload={
                "term": self._current_term,
                "success": True,
                "match_index": self._log.last_index(),
            }
        ))

    def _handle_append_entries_response(self, src: str, payload: Dict):
        term = payload["term"]
        success = payload["success"]
        match_index = payload["match_index"]

        if term > self._current_term:
            self._current_term = term
            self._state = RaftState.FOLLOWER
            self._voted_for = None
            self._last_heartbeat = time.time()
            return

        if self._state != RaftState.LEADER or term != self._current_term:
            return

        if success:
            self._match_index[src] = max(self._match_index.get(src, -1), match_index)
            self._next_index[src] = self._match_index[src] + 1
            self._maybe_advance_commit_index()
        else:
            self._next_index[src] = max(0, self._next_index.get(src, 0) - 1)
            self._send_append_entries(src)

    def _maybe_advance_commit_index(self):
        if self._state != RaftState.LEADER:
            return

        n = len(self.peers) + 1
        for n_idx in range(self._log.last_index(), self._commit_index, -1):
            if self._log.term_at(n_idx) != self._current_term:
                continue
            count = 1
            for peer in self.peers:
                if self._match_index.get(peer, -1) >= n_idx:
                    count += 1
            if count > n // 2:
                self._commit_index = n_idx
                break

    def _maybe_apply_committed(self):
        while self._last_applied < self._commit_index:
            self._last_applied += 1
            entry = self._log.get(self._last_applied)
            if entry:
                result = self.state_machine.apply(entry.command)
                self._notify_apply_callbacks(entry.index, result)

    def _notify_apply_callbacks(self, index: int, result: Any):
        with self._apply_lock:
            cb = self._apply_callbacks.pop(index, None)
        if cb:
            try:
                cb(result)
            except Exception:
                pass

    def submit_command(self, command: Any, callback: Callable = None) -> Optional[int]:
        with self._lock:
            if self._state != RaftState.LEADER:
                return None

            new_index = self._log.last_index() + 1
            entry = LogEntry(
                term=self._current_term,
                index=new_index,
                command=command,
            )
            self._log.append([entry])

            if callback:
                with self._apply_lock:
                    self._apply_callbacks[new_index] = callback

            self._match_index[self.node_id] = new_index

            for peer in self.peers:
                self._send_append_entries(peer)

            self._last_heartbeat_sent = 0.0

            self._maybe_advance_commit_index()
            self._maybe_apply_committed()

            return new_index

    def get_state(self) -> RaftState:
        with self._lock:
            return self._state

    def get_current_term(self) -> int:
        with self._lock:
            return self._current_term

    def get_leader_id(self) -> Optional[str]:
        with self._lock:
            if self._state == RaftState.LEADER:
                return self.node_id
            return None

    def is_leader(self) -> bool:
        with self._lock:
            return self._state == RaftState.LEADER
