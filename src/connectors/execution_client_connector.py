from web3 import Web3
from web3._utils.empty import Empty
from web3.exceptions import BlockNotFound, TransactionNotFound, NoABIFound, ABIFunctionNotFound, ABIEventFunctionNotFound
import json
import requests
from enum import Enum
from os import listdir
from typing import Union
from connectors.sql_database_connector import SqlDatabaseConnector


timeout = 60

# init token standards
TokenStandard = Enum("TokenStandard", [(token_standard.replace(
    ".json", ""), token_standard.replace(
    ".json", "")) for token_standard in listdir(f"src/token_standard") if token_standard.endswith(".json")])

token_standards = {}
for token_standard in TokenStandard:
    with open(f"src/token_standard/{token_standard.name}.json", "r") as f:
        token_standards[token_standard.name] = json.load(f)


class ExecutionClientConnector:

    def __init__(self,
                 execution_client_url,
                 etherscan_ip: str,
                 etherscan_api_key: str,
                 sql_db_connector: SqlDatabaseConnector,
                 contract_table_name: str
                 ) -> None:
        # execution client params
        self.execution_client_url = execution_client_url
        self.token_standards = token_standards
        # set etherscan api params
        self.etherscan_ip = etherscan_ip
        self.etherscan_api_key = etherscan_api_key
        # set sql db connector
        self.sql_db_connector = sql_db_connector
        self.contract_table_name = contract_table_name
        # init execution client
        self.execution_client = Web3(Web3.HTTPProvider(
            self.execution_client_url, request_kwargs={'timeout': timeout}))

    # Gossip methods

    def block_number(self):
        response = self.execution_client.eth.block_number

        return {"block_number": response}

    # State methods

    def default_account(self):
        response = self.execution_client.eth.default_account

        if type(response) is Empty:
            response = None

        return {"default_account": response}

    def default_block(self):
        response = self.execution_client.eth.default_block

        return {"default_block": response}

    def syncing(self):
        response = self.execution_client.eth.syncing

        if response == False:
            return {"syncing": False}

        return json.loads(Web3.to_json(response))

    def coinbase(self):
        response = self.execution_client.eth.coinbase

        return {"coinbase": response}

    def mining(self):
        response = self.execution_client.eth.mining

        return {"mining": response}

    def hashrate(self):
        response = self.execution_client.eth.hashrate

        return {"hashrate": response}

    def max_priority_fee(self):
        response = self.execution_client.eth.max_priority_fee

        return {"max_priority_fee": response}

    def accounts(self):
        response = self.execution_client.eth.accounts

        return {"accounts": response}

    def chain_id(self):
        response = self.execution_client.eth.chain_id

        return {"chain_id": response}

    def get_api_version(self):
        response = self.execution_client.api

        return {"api_version": response}

    def get_client_version(self):
        response = self.execution_client.client_version

        return {"client_version": response}

    def get_balance(self, wallet_address, block_identifier=None):
        wallet_address = Web3.to_checksum_address(wallet_address)

        if block_identifier is None:
            response = self.execution_client.eth.get_balance(wallet_address)
        else:
            response = self.execution_client.eth.get_balance(
                wallet_address, block_identifier)

        return {"balance": response}

    def get_block_number(self):
        response = self.execution_client.eth.get_block_number()

        return {"block_number": response}

    def get_storage_at(self, wallet_address, position, block_identifier=None):
        wallet_address = Web3.to_checksum_address(wallet_address)

        if block_identifier is None:
            response = self.execution_client.eth.get_storage_at(
                wallet_address, position)
        else:
            response = self.execution_client.eth.get_storage_at(
                wallet_address, position, block_identifier)

        return {"storage_value": response}

    def get_code(self, wallet_address):
        wallet_address = Web3.to_checksum_address(wallet_address)

        response = self.execution_client.eth.get_code(wallet_address)

        return {"bytecode": response}

    def get_transaction_count(self, wallet_address, block_identifier=None):
        wallet_address = Web3.to_checksum_address(wallet_address)

        if block_identifier is None:
            response = self.execution_client.eth.get_transaction_count(
                wallet_address)
        else:
            response = self.execution_client.eth.get_transaction_count(
                wallet_address, block_identifier)

        return {"transaction_count": response}

    def estimate_gas(self, from_address, to_address, value):
        from_address = Web3.to_checksum_address(from_address)
        to_address = Web3.to_checksum_address(to_address)

        response = self.execution_client.eth.estimate_gas({
            "to": to_address,
            "from": from_address,
            "value": value
        })

        return {"gas": response}

    # History methods

    def get_block_transaction_count(self, block_identifier):
        response = self.execution_client.eth.get_block_transaction_count(
            block_identifier)

        return {"block_transaction_count": response}

    def get_uncle_count(self, block_identifier):
        response = self.execution_client.eth.get_uncle_count(
            block_identifier)
        return {"uncle_count": response}

    def get_block(self, block_identifier=None, full_transactions=False):
        if block_identifier is None:
            response = self.execution_client.eth.get_block(
                self.execution_client.eth.default_block, full_transactions)
        else:
            response = self.execution_client.eth.get_block(
                block_identifier, full_transactions)

        return json.loads(Web3.to_json(response))

    def get_transaction(self, transaction_hash: str, decode_input: bool = True):
        response = self.execution_client.eth.get_transaction(
            transaction_hash)

        response = json.loads(Web3.to_json(response))

        if decode_input and response["input"] != "0x":
            contract_address = response["to"]

            contract_abi = self.get_contract_abi(contract_address)

            contract = self.execution_client.eth.contract(
                address=contract_address, abi=contract_abi)

            func_obj, func_params = contract.decode_function_input(
                response["input"])

            for param_name, param in func_params.items():
                if type(param) is int:
                    func_params[param_name] = str(param)
                if type(param) is bytes:
                    func_params[param_name] = f"0x{param.hex()}"

            decoded_input = {
                "function": func_obj,
                "params": func_params
            }

            response["input_decoded"] = decoded_input

        return response

    def get_transaction_by_block(self, block_identifier, transaction_index):
        response = self.execution_client.eth.get_transaction_by_block(
            block_identifier, transaction_index)
        return json.loads(Web3.to_json(response))

    def get_transaction_receipt(self, transaction_hash):
        response = self.execution_client.eth.get_transaction_receipt(
            transaction_hash)
        return json.loads(Web3.to_json(response))

    def get_uncle_by_block(self, block_identifier, uncle_index):
        response = self.execution_client.eth.get_uncle_by_block(
            block_identifier, uncle_index)
        return json.loads(Web3.to_json(response))

    # Contract methods

    def get_token_standard_abi(self, token_standard: TokenStandard):
        return self.token_standards[token_standard.name]

    def get_contract_abi(self, contract_address: str):
        """
        Return contract abi.
        Query from db if in db else query from etherscan (no other way)
        """
        if self.sql_db_connector.is_contract_in_db(self.contract_table_name, contract_address):
            contract_data = self.sql_db_connector.query_all_contract_data(
                self.contract_table_name, contract_address)
            return contract_data["abi"]
        else:
            # contract abi can not be retrieved from blockchain (not with get_code()) -> etherscan is needed
            params = {
                "module": "contract",
                "action": "getabi",
                "address":  contract_address,
                "apikey": self.etherscan_api_key
            }
            response = requests.get(self.etherscan_ip, params=params)

            if response.json()["status"] == "0":
                raise NoABIFound

            contract_abi = json.loads(response.json()["result"])

            contract_metadata = self.get_contract_metadata(
                contract_address, contract_abi)

            contract_implemented_token_standards = self.get_contract_implemented_token_standards(
                contract_address, contract_abi)

            # insert data into db
            self.sql_db_connector.insert_contract_data(
                self.contract_table_name, contract_address, contract_metadata, contract_implemented_token_standards, contract_abi)

            return contract_abi

    def get_contract_implemented_token_standards(self, contract_address: str, contract_abi=None):
        # TODO: improve filter function with more than function name
        if contract_abi is None:
            contract_abi = self.get_contract_abi(contract_address)

        contract_abi = [
            contract_function_abi for contract_function_abi in contract_abi if "name" in contract_function_abi.keys()]

        implemented_token_standards = {}

        for token_standard_name, token_standard_abi in self.token_standards.items():
            # implemented flag
            implemented = True

            for token_standard_function_abi in token_standard_abi:
                contract_function_filter = [
                    contract_function for contract_function in contract_abi if contract_function["name"] == token_standard_function_abi["name"]]

                if len(contract_function_filter) == 0:
                    implemented = False
                    break

            implemented_token_standards[token_standard_name] = implemented

        return implemented_token_standards

    def get_all_contract_functions(self, contract_address: str, as_abi: bool = True):
        """
        returns all contract functions
        if as_abi = True -> return abi of all functions
        if as_abi = False -> return names of all functions
        """
        contract_abi = self.get_contract_abi(contract_address)

        if as_abi:
            contract_functions = [
                func for func in contract_abi if func["type"] == "function"]
        else:
            contract_functions = [
                func["name"] for func in contract_abi if func["type"] == "function"]

        return contract_functions

    def get_all_contract_events(self, contract_address: str,  as_abi: bool = True):
        """
        returns all contract events
        if as_abi = True -> return abi of all events
        if as_abi = False -> return names of all events
        """
        contract_abi = self.get_contract_abi(contract_address)

        if as_abi:
            contract_events = [
                event for event in contract_abi if event["type"] == "event"]
        else:
            contract_events = [
                event["name"] for event in contract_abi if event["type"] == "event"]

        return contract_events

    def execute_contract_function(self, contract_address: str, function_name: str, contract_abi=None,  *function_args):
        if contract_abi is None:
            contract_abi = self.get_contract_abi(contract_address)

        contract = self.execution_client.eth.contract(
            Web3.to_checksum_address(contract_address), abi=contract_abi)

        contract_function = contract.functions[function_name]

        response = contract_function(*function_args).call()

        return response

    def get_contract_events_by_name(self,
                                    contract_address: str,
                                    event_name: str,
                                    from_block: Union[int, str] = 0,
                                    to_block: Union[int, str] = "latest",
                                    argument_filters: dict = {}):
        contract_abi = self.get_contract_abi(contract_address)

        contract = self.execution_client.eth.contract(
            Web3.to_checksum_address(contract_address), abi=contract_abi)

        contract_event_list = []

        # create event object
        contract_event = contract.events[event_name]

        batch_from_block = from_block
        batch_to_block = to_block

        more_batches = True

        # get all data in batches
        while more_batches:
            try:
                # try if batch size is in limits (<10000)
                event_filter = contract_event.create_filter(
                    fromBlock=batch_from_block, toBlock=batch_to_block, argument_filters=argument_filters)

                response = event_filter.get_all_entries()

                contract_event_list.extend(
                    json.loads(Web3.to_json(response)))

                if batch_to_block == to_block:
                    more_batches = False
                else:
                    batch_from_block = batch_to_block + 1
                    batch_to_block = to_block

            except ValueError as e:
                # change batch intervall if intervall is bigger than limits
                batch_from_block = int(e.args[0]["data"]["from"], 16)
                batch_to_block = int(e.args[0]["data"]["to"], 16)
                pass

        return contract_event_list

    def get_token_transfers(self,
                            contract_address: str,
                            from_block: Union[int, str] = 0,
                            to_block: Union[int, str] = "latest",
                            argument_filters: dict = {}):
        """
        will not work with all contracts because of missing 'Transfer' event in abi
        """
        return self.get_contract_events_by_name(contract_address, "Transfer", from_block, to_block, argument_filters)

    def get_contract_events(self,
                            contract_address: str,
                            from_block: Union[int, str] = 0,
                            to_block: Union[int, str] = "latest"):
        contract_event_list = []

        # create event object

        batch_from_block = from_block
        batch_to_block = to_block

        more_batches = True

        # get all data in batches
        while more_batches:
            try:
                # try if batch size is in limits (<10000)
                event_filter = self.execution_client.eth.filter({
                    "fromBlock": batch_from_block,
                    "toBlock": batch_to_block,
                    "address": contract_address
                })

                response = event_filter.get_all_entries()

                contract_event_list.extend(
                    json.loads(Web3.to_json(response)))

                if batch_to_block == to_block:
                    more_batches = False
                else:
                    batch_from_block = batch_to_block + 1
                    batch_to_block = to_block

            except ValueError as e:
                # change batch intervall if intervall is bigger than limits
                batch_from_block = int(e.args[0]["data"]["from"], 16)
                batch_to_block = int(e.args[0]["data"]["to"], 16)
                pass

        return contract_event_list

    def get_contract_deploy_block(self, contract_address: str):
        try:
            event_filter = self.execution_client.eth.filter({
                "fromBlock": 0,
                "toBlock": "latest",
                "address": contract_address
            })

            response = event_filter.get_all_entries()

            if len(response) == 0:
                return None

            response = response[0]["blockNumber"]

            return response

        except ValueError as e:
            if "data" in e.args[0].keys():
                return int(e.args[0]["data"]["from"], 16)
            else:
                return None

    def get_contract_metadata(self, contract_address: str, contract_abi=None):
        try:
            token_name = self.execute_contract_function(
                contract_address, "name", contract_abi)
        except:
            token_name = None
        try:
            token_symbol = self.execute_contract_function(
                contract_address, "symbol", contract_abi)
        except:
            token_symbol = None
        try:
            deploy_block = self.get_contract_deploy_block(contract_address)
        except:
            deploy_block = None
        try:
            token_total_supply = self.execute_contract_function(
                contract_address, "totalSupply", contract_abi)
        except:
            token_total_supply = None

        return {
            "address": contract_address,
            "name": token_name,
            "symbol": token_symbol,
            "block_deployed": deploy_block,
            "total_supply": token_total_supply
        }
