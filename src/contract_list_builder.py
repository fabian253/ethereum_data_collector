import json
import os

if __name__ == "__main__":
    with open("src/process_data/contract_list_etherscan.json", "r") as f:
        contract_list_etherscan: list = json.load(f)
    with open("src/process_data/contract_list_opensea.json", "r") as f:
        contract_list_opensea: list = json.load(f)

    contract_list = []
    if os.path.isfile("src/process_data/contract_list.json"):
        with open("src/process_data/contract_list.json", "r") as f:
            contract_list: list = json.load(f)

    contract_address_list = []
    if len(contract_list) != 0:
        contract_address_list = [c["address"] for c in contract_list]

    etherscan_only_contract_list = [i for i in contract_list_etherscan if i["address"]
                                    not in [i2["address"] for i2 in contract_list_opensea]]

    new_contract_list = contract_list_opensea.copy()
    new_contract_list.extend(etherscan_only_contract_list)

    for contract in new_contract_list:
        if contract["address"] not in contract_address_list:
            contract["source"] = []

            if contract["address"] in [i["address"] for i in contract_list_etherscan]:
                contract["source"].append("etherscan")

            if contract["address"] in [i["address"] for i in contract_list_opensea]:
                contract["source"].append("opensea")

            contract["collected"] = False
            contract["error"] = False

            contract_list.append(contract)

    with open("src/process_data/contract_list.json", "w") as f:
        json.dump(contract_list, f, indent=4)
