from web3 import Web3
from web3.beacon.main import Beacon
import json


class ConsensusClientConnector:

    def __init__(self, consensus_client_ip: str, consensus_client_port: int) -> None:
        self.client_ip = consensus_client_ip
        self.client_port = consensus_client_port
        # init consensus client
        self.consensus_client = Beacon(
            f"http://{self.client_ip}:{self.client_port}")

    # Beacon methods

    def get_genesis(self):
        response = self.consensus_client.get_genesis()

        return json.loads(Web3.to_json(response))

    def get_hash_root(self, state_id="head"):
        response = self.consensus_client.get_hash_root(state_id)

        return json.loads(Web3.to_json(response))

    def get_fork_data(self, state_id="head"):
        response = self.consensus_client.get_fork_data(state_id)

        return json.loads(Web3.to_json(response))

    def get_finality_checkpoint(self, state_id="head"):
        response = self.consensus_client.get_finality_checkpoint(state_id)

        return json.loads(Web3.to_json(response))

    def get_validators(self, state_id="head"):
        response = self.consensus_client.get_validators(state_id)

        return json.loads(Web3.to_json(response))

    def get_validator(self, validator_id, state_id="head"):
        response = self.consensus_client.get_validator(validator_id, state_id)

        return json.loads(Web3.to_json(response))

    def get_validator_balances(self, state_id="head"):
        response = self.consensus_client.get_validator_balances(state_id)

        return json.loads(Web3.to_json(response))

    def get_epoch_committees(self, state_id="head"):
        response = self.consensus_client.get_epoch_committees(state_id)

        return json.loads(Web3.to_json(response))

    def get_block_headers(self):
        response = self.consensus_client.get_block_headers()

        return json.loads(Web3.to_json(response))

    def get_block_header(self, block_id):
        response = self.consensus_client.get_block_header(block_id)

        return json.loads(Web3.to_json(response))

    def get_block(self, block_id):
        response = self.consensus_client.get_block(block_id)

        return json.loads(Web3.to_json(response))

    def get_block_root(self, block_id):
        response = self.consensus_client.get_block_root(block_id)

        return json.loads(Web3.to_json(response))

    def get_block_attestations(self, block_id):
        response = self.consensus_client.get_block_attestations(block_id)

        return json.loads(Web3.to_json(response))

    def get_attestations(self):
        response = self.consensus_client.get_attestations()

        return json.loads(Web3.to_json(response))

    def get_attester_slashings(self):
        response = self.consensus_client.get_attester_slashings()

        return json.loads(Web3.to_json(response))

    def get_proposer_slashings(self):
        response = self.consensus_client.get_proposer_slashings()

        return json.loads(Web3.to_json(response))

    def get_voluntary_exits(self):
        response = self.consensus_client.get_voluntary_exits()

        return json.loads(Web3.to_json(response))

    # Config methods

    def get_fork_schedule(self):
        response = self.consensus_client.get_fork_schedule()

        return json.loads(Web3.to_json(response))

    def get_spec(self):
        response = self.consensus_client.get_spec()

        return json.loads(Web3.to_json(response))

    def get_deposit_contract(self):
        response = self.consensus_client.get_deposit_contract()

        return json.loads(Web3.to_json(response))

    # Debug methods

    # not working with web3 version 5.31.3
    def get_beacon_state(self, state_id="head"):
        response = self.consensus_client.get_beacon_state(state_id)

        return json.loads(Web3.to_json(response))

    # not working with web3 version 5.31.3
    def get_beacon_heads(self):
        response = self.consensus_client.get_beacon_heads()

        return json.loads(Web3.to_json(response))

    # Node methods

    def get_node_identity(self):
        response = self.consensus_client.get_node_identity()

        return json.loads(Web3.to_json(response))

    def get_peers(self):
        response = self.consensus_client.get_peers()

        return json.loads(Web3.to_json(response))

    def get_peer(self, peer_id):
        response = self.consensus_client.get_peer(peer_id)

        return json.loads(Web3.to_json(response))

    def get_health(self):
        response = self.consensus_client.get_health()

        return {"health": response}

    def get_version(self):
        response = self.consensus_client.get_version()

        return json.loads(Web3.to_json(response))

    def get_syncing(self):
        response = self.consensus_client.get_syncing()

        return json.loads(Web3.to_json(response))
