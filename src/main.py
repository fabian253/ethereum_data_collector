from connectors import SqlDatabaseConnector, ExecutionClientConnector, ConsensusClientConnector
import config
import db_params.sql_tables as tables
from web3.exceptions import NoABIFound, ABIFunctionNotFound
import json
from typing import Union
import config
import logging

filter_coontract_list = True

logging.basicConfig(level=logging.INFO)

# init sql database connector
sql_db_connector = SqlDatabaseConnector(
    config.SQL_DATABASE_HOST,
    config.SQL_DATABASE_PORT,
    config.SQL_DATABASE_USER,
    config.SQL_DATABASE_PASSWORD,
    config.SQL_DATABASE_NAME,
    [tables.CONTRACT_TABLE, tables.TRANSACTION_TABLE]
)

# init execution client
execution_client_url = f"http://{config.EXECUTION_CLIENT_IP}:{config.EXECUTION_CLIENT_PORT}"
execution_client = ExecutionClientConnector(
    execution_client_url, config.ETHERSCAN_URL, config.ETHERSCAN_API_KEY, sql_db_connector, config.SQL_DATABASE_TABLE_CONTRACT)

# TODO: remove when node is fully synced -> currently used for contract endpoints only
# init infura execution client
infura_execution_client_url = f"{config.INFURA_URL}/{config.INFURA_API_KEY}"
infura_execution_client = ExecutionClientConnector(
    infura_execution_client_url, config.ETHERSCAN_URL, config.ETHERSCAN_API_KEY, sql_db_connector, config.SQL_DATABASE_TABLE_CONTRACT)

# init conensus client
consensus_client = ConsensusClientConnector(
    config.CONSENCUS_CLIENT_IP, config.CONSENSUS_CLIENT_PORT)


def insert_contract_transactions(contract_address: str,
                                 from_block: Union[int, str, None] = 0,
                                 to_block: Union[int,
                                                 str, None] = "latest",
                                 from_address: Union[str, None] = None,
                                 to_address: Union[str, None] = None,
                                 value: Union[int, None] = None,
                                 token_id: Union[int, None] = None,
                                 ):
    """
    Insert contract transactions into sql database.

    * from_block (optional): first block to filter from
    * to_block (optional): last block to filter to
    * from_address (optional): from address of the transfer
    * to_address (optional): to address of the transfer
    * value (optional): value of the transfer
    * token_id (optional): ID of the token

    Transfers can be filtered either by 'value' or 'token_id' depending on the token standard (ERC20, ERC721, ...) of the contract. It is not possible to provide both values at the same time.
    """
    if value is not None and token_id is not None:
        raise ValueError(
            "It is not possible to pass both 'value' and 'token_id' at the same time.")

    argument_filters = {}
    if from_address is not None:
        argument_filters["from"] = from_address
    if to_address is not None:
        argument_filters["to"] = to_address
    if value is not None:
        argument_filters["value"] = value
    if token_id is not None:
        argument_filters["tokenId"] = token_id

    # TODO: remove infura when syced
    try:
        transactions = infura_execution_client.get_token_transfers(
            contract_address,
            from_block,
            to_block,
            argument_filters
        )
    except NoABIFound:
        raise ValueError(
            f"Contract not found (contract_address: {contract_address})")
    except ABIFunctionNotFound:
        raise ValueError(
            f"Contract or ABI function not found (contract_address: {contract_address})")

    logging.info(f"Contract transactions collected")

    for index, transaction in enumerate(transactions):
        transactions[index] = {
            "transaction_hash": transaction["transactionHash"],
            "contract_address": transaction["address"],
            "from_address": transaction["args"]["from"],
            "to_address": transaction["args"]["to"],
            "block_number": transaction["blockNumber"]
        }
        if "tokenId" in transaction["args"]:
            transactions[index]["token_id"] = transaction["args"]["tokenId"]
        if "value" in transaction["args"]:
            transactions[index]["value"] = transaction["args"]["value"]

    sql_db_connector.insert_many_transaction_data(
        config.SQL_DATABASE_TABLE_TRANSACTION, transactions)

    logging.info(f"Contract transactions inserted in db")


if __name__ == "__main__":
    # load contract list
    with open("src/process_data/dao_contract_list.json", "r") as f:
        contract_list = json.load(f)

    logging.info(f"Contracts loaded: {len(contract_list)} contracts")

    for index, contract in enumerate([contract for contract in contract_list]):
        try:
            logging.info(
                f"Contract transaction collection started: {contract['address']} [{index+1}/{len(contract_list)}]")

            # filter contract list
            if filter_coontract_list:
                if not contract["collected"] and not contract["error"]:
                    insert_contract_transactions(contract['address'])
                else:
                    logging.info(
                        f"Contract transactions already collected and in db")
            else:
                insert_contract_transactions(contract['address'])

            logging.info(
                f"Contract transaction collection done: {contract['address']} [{index+1}/{len(contract_list)}]")
        except Exception as e:
            logging.error(
                f"Error while collecting contract transactions: {contract['address']} [{index+1}/{len(contract_list)}]")
            # raise e
