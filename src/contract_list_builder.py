import json

if __name__ == "__main__":
    with open("src/process_data/contract_list_etherscan.json", "r") as f:
        contract_list_etherscan: list = json.load(f)
    with open("src/process_data/contract_list_opensea.json", "r") as f:
        contract_list_opensea: list = json.load(f)

    for contract in contract_list_etherscan:
        contract["source"] = []

    etherscan_only_contract_list = [i for i in contract_list_etherscan if i["address"]
                                    not in [i2["address"] for i2 in contract_list_opensea]]

    contract_list = contract_list_opensea.copy()
    contract_list.extend(etherscan_only_contract_list)

    for contract in contract_list:
        contract["source"] = []

        if contract["address"] in [i["address"] for i in contract_list_etherscan]:
            contract["source"].append("etherscan")

        if contract["address"] in [i["address"] for i in contract_list_opensea]:
            contract["source"].append("opensea")

    with open("src/process_data/contract_list.json", "w") as f:
        json.dump(contract_list, f, indent=4)
