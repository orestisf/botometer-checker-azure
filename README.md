# botometer-checker-azure
A dockerized application that reads tweet objects from an Azure Event Hub, uses the [Botometer](https://botometer.osome.iu.edu/) API  to check the account that posted each tweet for the probability to demonstrate a bot-like behavior and stores the result in a Cosmos DB container.

## Setting environment variables

### <b>Twitter authorization</b>: API (consumer) key, secret and access token, secret for the twitter app that will be used to make requests to the botometer API:

- CONSUMER_KEY
- CONSUMER_SECRET
- ACCESS_KEY
- ACCESS_SECRET
### URL, Primary Key, and database name for the <b>Cosmos DB database</b> that is used to store the botometer results and the daily botometer API calls:
- COSMOSDB_URL
- COSMOSDB_KEY
- COSMOSDB_DB_NAME
### <b>Azure Blob storage</b> credentials: Connection string and blob container name which is used to store checkpoints while processing events the from Azure Event Hub (see also [here](https://docs.microsoft.com/en-us/python/api/overview/azure/eventhub-checkpointstoreblob-aio-readme?view=azure-python)):
- STORAGE_CONNECTION_STRING
- BLOB_CONTAINER_NAME
### <b>Event Hub</b> credentials: Connection string, event hub name and consumer group which are used to read tweets from:
- EVENTHUB_CONNECTION_STRING
- EVENTHUB_NAME
- EVENTHUB_CONSUMER_GROUP
### <b>Cosmos DB</b> container names: For Botometer index which stores the botometer results for checked users and Botometer daily counter which stores the number of API calls made for a given day:
- COSMOSDB_BOTOMETER_INDEX_CONTAINER_NAME
- COSMOSDB_BOTOMETER_COUNTER_CONTAINER_NAME
### <b>Rapidapi</b> key: Set the personal RAPIDAPI key obtained from https://rapidapi.com.
- RAPIDAPI_KEY
### <b>Botometer session limit</b>: Set the max number of Botometer API requests to be made in a single session.
- BOTOMETER_SESSION_LIMIT