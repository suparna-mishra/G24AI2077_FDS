from os import environ
from flask import Flask, request, jsonify
from threading import Lock
import requests
import time
import json
from werkzeug.utils import url_quote
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

class VectorClock:
    def __init__(self, node_id, node_count):
        """Initialize with node ID and total node count"""
        if not isinstance(node_count, int) or node_count <= 0:
            raise ValueError("node_count must be a positive integer")
            
        self.node_id = node_id
        self.clock = [0] * node_count  # Initialize vector for all nodes
        self.lock = Lock()
    
    def increment(self):
        """Increment our own clock position"""
        with self.lock:
            self.clock[self.node_id] += 1
            return self.clock.copy()
    
    def update(self, received_clock):
        """Merge with another clock vector"""
        with self.lock:
            if len(received_clock) != len(self.clock):
                raise ValueError("Clock vector length mismatch")
            self.clock = [max(ours, theirs) 
                         for ours, theirs in zip(self.clock, received_clock)]
    
    def is_causally_ready(self, received_clock):
        """Check if we've seen all prerequisite events"""
        if len(received_clock) != len(self.clock):
            return False
            
        for i in range(len(self.clock)):
            if i == self.node_id:
                if received_clock[i] != self.clock[i] + 1:
                    return False
            elif received_clock[i] > self.clock[i]:
                return False
        return True
    def __init__(self, node_id, node_count):
        self.node_id = node_id
        self.clock = [0] * node_count
        self.lock = Lock()
    
    def increment(self):
        with self.lock:
            self.clock[self.node_id] += 1
            return self.clock.copy()
    
    def update(self, received_clock):
        with self.lock:
            for i in range(len(self.clock)):
                self.clock[i] = max(self.clock[i], received_clock[i])
    
    def is_causally_ready(self, received_clock):
        """Check if all causal dependencies are satisfied"""
        # For each node, our clock must be >= the received clock for all other nodes
        # And equal for our own node (to maintain ordering of our own writes)
        for i in range(len(self.clock)):
            if i == self.node_id:
                if received_clock[i] != self.clock[i] + 1:
                    return False
            else:
                if received_clock[i] > self.clock[i]:
                    return False
        return True


    def __init__(self, node_id, nodes):
        self.node_id = node_id
        self.nodes = nodes
        self.vector_clock = VectorClock(node_id, len(nodes))
        self.data = {}
        self.buffer = []
        self.lock = Lock()
    
    def local_write(self, key, value):
        clock = self.vector_clock.increment()
        with self.lock:
            self.data[key] = (value, clock)
        self.replicate(key, value, clock)
        return clock
    
    def replicate(self, key, value, clock):
    
        for i, node_url in enumerate(self.nodes):
            if i != self.node_id:
                try:
                    response = requests.post(
                        f"{node_url}/replicate",
                        json={
                            'key': key,
                            'value': value,
                            'clock': clock,
                            'sender': self.node_id
                        },
                        timeout=1.0
                )
                    if response.status_code != 200:
                        logger.warning(f"Replication to node {i} failed")
                except Exception as e:
                    logger.warning(f"Replication to node {i} error: {str(e)}")

    def handle_replication(self, key, value, clock, sender):
        with self.lock:
            if self.vector_clock.is_causally_ready(clock):
                self.data[key] = (value, clock)
                self.vector_clock.update(clock)
                self.process_buffered()
                return True
            else:
                self.buffer.append((key, value, clock, sender))
                return False
    
    def process_buffered(self):
        processed = []
        for i, (key, value, clock, sender) in enumerate(self.buffer):
            if self.vector_clock.is_causally_ready(clock):
                self.data[key] = (value, clock)
                self.vector_clock.update(clock)
                processed.append(i)
        self.buffer = [item for i, item in enumerate(self.buffer) if i not in processed]

class KVStore:
    def __init__(self, node_id, nodes):
        if not isinstance(nodes, list):
            raise ValueError("nodes must be a list of URLs")
        
        self.node_id = node_id
        self.nodes = nodes
        self.vector_clock = VectorClock(node_id, len(nodes))  # Now safe
        self.data = {}
        self.pending_writes = []
        self.lock = Lock()
        logging.info(f"Initialized node {node_id} with {len(nodes)} peers")


    def handle_replication(self, key, value, clock, sender):
        with self.lock:
            # Check if all causal dependencies are satisfied
            if self.vector_clock.is_causally_ready(clock):
                self._apply_write(key, value, clock)
                self._process_pending()
                return True
            else:
                # Store write until dependencies are met
                self.pending_writes.append((key, value, clock, sender))
                return False

    def _apply_write(self, key, value, clock):
        """Apply a write and update vector clock"""
        self.data[key] = (value, clock)
        self.vector_clock.update(clock)

    def _process_pending(self):
        """Process any pending writes that now have satisfied dependencies"""
        processed = []
        for i, (key, value, clock, sender) in enumerate(self.pending_writes):
            if self.vector_clock.is_causally_ready(clock):
                self._apply_write(key, value, clock)
                processed.append(i)
        # Remove processed writes
        self.pending_writes = [w for i, w in enumerate(self.pending_writes) 
                             if i not in processed]

# Initialize the store
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_nodes_config():
    DEFAULT_NODES = [
        "http://node0:5000",
        "http://node1:5000",
        "http://node2:5000"
    ]
    
    nodes_str = environ.get('NODES', '')
    try:
        if not nodes_str.strip():
            return DEFAULT_NODES
            
        nodes = json.loads(nodes_str)
        if not isinstance(nodes, list):
            raise ValueError("NODES must be a list")
        return nodes
        
    except (json.JSONDecodeError, ValueError):
        return DEFAULT_NODES

def load_config():
    """Safely load node configuration"""
    try:
        node_id = int(environ.get('NODE_ID', 0))
        
        # Get nodes list with validation
        nodes_str = environ.get('NODES', '').strip()
        if not nodes_str:
            nodes = []
        else:
            nodes = json.loads(nodes_str)
            if not isinstance(nodes, list):
                raise ValueError("NODES must be a JSON array")
        
        return node_id, nodes
    
    except (ValueError, json.JSONDecodeError) as e:
        logging.error(f"Configuration error: {e}")
        # Default to single node if configuration fails
        return 0, []

# Initialize configuration
NODE_ID, NODES = load_config()
logger.info(f"Node {NODE_ID} initialized with peers: {NODES}")
store = KVStore(NODE_ID, NODES)

@app.route('/write', methods=['POST'])
def write():
    data = request.json
    clock = store.local_write(data['key'], data['value'])
    return jsonify({'status': 'success', 'clock': clock})

@app.route('/replicate', methods=['POST'])
def replicate():
    data = request.json
    if store.handle_replication(data['key'], data['value'], data['clock'], data['sender']):
        return jsonify({'status': 'processed'})
    else:
        return jsonify({'status': 'buffered'})

@app.route('/read/<key>', methods=['GET'])
def read(key):
    with store.lock:
        if key in store.data:
            return jsonify({
                'value': store.data[key][0],
                'clock': store.data[key][1]
            })
        return jsonify({'error': 'key not found'}), 404
    
@app.route('/health')
def health():
    return jsonify({'status': 'healthy'})    

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)