# execution client
EXECUTION_CLIENT_IP = "134.155.111.231"  # for local testing
# EXECUTION_CLIENT_IP = "172.17.0.3"  # for server deployment
EXECUTION_CLIENT_PORT = 8545
# consensus client
CONSENCUS_CLIENT_IP = "134.155.111.231"  # for local testing
# CONSENCUS_CLIENT_IP = "172.17.0.4"  # for server deployment
CONSENSUS_CLIENT_PORT = 3500

# to get a string like this run: openssl rand -hex 32
API_AUTHENTICATION_SECRET_KEY = "c1c38c4d62b5bbdfca5691baae25e0babbbaea105aa75ba3189be276a2eb5817"
API_AUTHENTICATION_ALGORITHM = "HS256"
API_AUTHENTICATION_ACCESS_TOKEN_EXPIRE_MINUTES = 525600  # 1year in minutes

# etherscan
ETHERSCAN_URL = "https://api.etherscan.io/api"
ETHERSCAN_API_KEY = "NZW3WAVANMS9WHFJZSB69MQFJEZ1D1E54R"

# infura
INFURA_URL = "https://mainnet.infura.io/v3"
INFURA_API_KEY = "c2762ad3b91949a099c826439f9dc5c6"

# mysql database
SQL_DATABASE_HOST = "134.155.111.231"
SQL_DATABASE_PORT = 3307
SQL_DATABASE_USER = "root"
SQL_DATABASE_PASSWORD = "admin"
SQL_DATABASE_NAME = "ethereum_api"
SQL_DATABASE_TABLE_CONTRACT = "contract"
SQL_DATABASE_TABLE_TRANSACTION = "transaction"
