## General libraries
import os
import sys
import traceback
from datetime import datetime, timezone
import asyncio
import json
import uuid
import logging

# Botometer library
import botometer

# Azure related libraries
from azure.cosmos import exceptions, CosmosClient, PartitionKey
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore

# 1.Twitter app credentials (this is the twitterStreamListener-dev-01 app)
twitter_app_auth = {
    'consumer_key': os.environ['CONSUMER_KEY'],
    'consumer_secret': os.environ['CONSUMER_SECRET'],
    'access_key': os.environ['ACCESS_KEY'],
    'access_secret': os.environ['ACCESS_SECRET'],
  }
  
# 2.Cosmos DB credentials
cosmosdb_url = os.environ['COSMOSDB_URL']
cosmosdb_key = os.environ['COSMOSDB_KEY']
cosmosdb_db_name = os.environ['COSMOSDB_DB_NAME']

# 3.Azure Blob storage credentials
storage_connection_string = os.environ['STORAGE_CONNECTION_STRING']
blob_container_name = os.environ['BLOB_CONTAINER_NAME']

# 4.Event Hub credentials
eventhub_connection_string = os.environ['EVENTHUB_CONNECTION_STRING']
eventhub_name = os.environ['EVENTHUB_NAME']
eventhub_consumer_group = os.environ['EVENTHUB_CONSUMER_GROUP']

# 5.Cosmos DB container names
cosmosdb_botometer_index_container_name = os.environ['COSMOSDB_BOTOMETER_INDEX_CONTAINER_NAME']
cosmosdb_botometer_counter_container_name = os.environ['COSMOSDB_BOTOMETER_COUNTER_CONTAINER_NAME']

# 6.Set the RAPIDAPI key from https://rapidapi.com/
rapidapi_key = os.environ['RAPIDAPI_KEY']

# 7.Set max number of botometer api calls to be made in a day
botometer_session_limit = os.environ['BOTOMETER_SESSION_LIMIT']

## Initialize Cosmos DB clients

# 1.Initialize Cosmosdb client and db
cosmosdbClient = CosmosClient(cosmosdb_url, cosmosdb_key)
cosmosdbDatabase = cosmosdbClient.get_database_client(cosmosdb_db_name)

# 2.Initialize Tweet container client
botometer_index_container_client = cosmosdbDatabase.get_container_client(cosmosdb_botometer_index_container_name)

# 3.Initialize Botometer service daily counter usage container client
botometerDailyCounterClient = cosmosdbDatabase.get_container_client(cosmosdb_botometer_counter_container_name)

# To be used with COSMOSDB_BOTOMETER_COUNTER_CONTAINER_NAME: Create item to store the daily botometer API utilization.
# Normally it should execute every day the checker runs for the first time.
def create_item(container, date, checkCount):
    print('Creating Item for the day')
    item = {"id":str(uuid.uuid4()),
            "date":date,
            "checkCount":checkCount}
    response = container.create_item(item)
    print("item is", item)
    print("Created item Id: {0}, date: {1}, checkCount: {2}".format(response['id'], response['date'], response['checkCount']))

async def replace_item(container, id, checkCount):
    read_item = container.read_item(item = id, partition_key = id)
    read_item['checkCount'] = checkCount
    response = container.replace_item(item=read_item, body=read_item)
    print("Replaced item Id: {0}, new checkCount is: {1}".format(response['id'], response['checkCount']))

async def on_partition_initialize(partition_context):
    print("Partition: {} has been initialized.".format(partition_context.partition_id))

async def on_partition_close(partition_context, reason):
    print("Partition: {} has been closed, reason for closing: {}.".format(partition_context.partition_id,reason))

async def on_error(partition_context, error):
    if partition_context:
        print("An exception: {} occurred during receiving from Partition: {}.".format(partition_context.partition_id,error))
    else:
        print("An exception: {} occurred during the load balance process.".format(error))

async def botometer_check_account(userAccountIdList):
    # Receives a list of twitter account ids.
    # Checks each ID using the check_account() and returns a list of checked accounts.
    # For each account successfully checked, increments the botometerSessionRequestCount.
    global botometerSessionRequestCount
    print("Current session count:", botometerSessionRequestCount)
    bom = botometer.Botometer(wait_on_ratelimit=True,
                        rapidapi_key=rapidapi_key,
                        consumer_key=twitter_app_auth['consumer_key'],
                        consumer_secret=twitter_app_auth['consumer_secret'],
                        access_token=twitter_app_auth['access_key'],
                        access_token_secret=twitter_app_auth['access_secret'])
    resultcontainer = []
    for i in userAccountIdList:
        print("Sending request for user id",i['user_id_str'])
        result = bom.check_account(i['user_id_str'])
        #print("result for user id",i['user_id_str'],"is:", result)
        resultcontainer.append(result)
        botometerSessionRequestCount += 1
    print("result list:", resultcontainer)
    return resultcontainer

async def createUserList(eventBatch):
    userList = []
    for i in eventBatch:
        userId = {}
        eventBodyJson = i.body_as_json()
        userId['user_id_str'] = eventBodyJson[0]['user_id_str']
        userId['user_screenname'] = eventBodyJson[0]['user_screenname']
        userList.append(userId)
    return userList

async def upsertUserList(userList):
    for i in userList:
        for k,v in i.items():
            #print(k, 'corresponds to', v)
            if k == 'cap':
                #print("found cap")
                print(v)
                cap = v
            if k == 'display_scores':
                #print("scores")
                display_scores_english = v['english']
                display_scores_universal = v['universal']
            if k == 'raw_scores':
                #print("scores")
                raw_scores_english = v['english']
                raw_scores_universal = v['universal']
            if k == 'user':
                #print("user")
                user_majority_lang = v['majority_lang']
                user_id_str = v['user_data']['id_str']
                user_screen_name = v['user_data']['screen_name']

        botometer_index_container_client.upsert_item({
        'id': user_id_str,
        'user_id': user_id_str,
        'user_screen_name': user_screen_name,
        'user_majority_lang': user_majority_lang,
        'cap': cap,
        'display_scores_english': display_scores_english,
        'display_scores_universal': display_scores_universal,
        'raw_scores_english': raw_scores_english,
        'raw_scores_universal': raw_scores_universal,
        'last_checked': datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        })


async def receive_batch():
    checkPointStore = BlobCheckpointStore.from_connection_string(storage_connection_string,blob_container_name)
    eventConsumerClient = EventHubConsumerClient.from_connection_string(conn_str=eventhub_connection_string,
                                                                          consumer_group=eventhub_consumer_group,
                                                                          eventhub_name=eventhub_name,
                                                                          checkpoint_store=checkPointStore)
    async with eventConsumerClient:
        await eventConsumerClient.receive_batch(
            on_event_batch=on_event_batch,
            on_partition_initialize=on_partition_initialize,
            on_partition_close=on_partition_close,
            on_error=on_error,
            max_batch_size=100,
            starting_position= "-1")

async def on_event_batch(partition_context, event_batch):
    print("Partition {}, Received count: {}".format(partition_context.partition_id, len(event_batch)))
    global botometerSessionRequestCount
    if int(botometerSessionRequestCount) > int(botometer_session_limit):
        print("Daily limit reached, terminating program")
        sys.exit("daily counter reached why not")
    else:
        print("proceed for now")
    # Extract user_id_str, user_screenname from every tweet in the batch into the userId list.
    userList = createUserList(event_batch)
    userList = await userList

    print("User list has:",len(userList))
    print("User list contents:",userList)

    # Get distinct users in list, in case the batch contains >1 tweets from the same user
    distinctUserList = list({v['user_screenname']:v for v in userList}.values())
    print('Distinct users in batch:',len(distinctUserList),"Enumerating:",distinctUserList)

    # Query CosmosDB for any users that have already been checked in Botometer
    queryparam = "('"+"','".join("{user_id_str}".format_map(v) for v in distinctUserList)+"')"
    query="SELECT r.user.user_data.id_str AS user_id_str, r.user.user_data.screen_name AS user_screenname FROM r WHERE r.user.user_data.id_str IN" + queryparam
    checkedUsers = list(botometer_index_container_client.query_items(query=query,enable_cross_partition_query=True))

    # Trim down distinctUserList to only keep unchecked users
    if checkedUsers: print("Found", len(checkedUsers),"previously checked users in database.")
    distinctUserList = [i for i in distinctUserList if i not in checkedUsers]
    if distinctUserList: print("found",len(distinctUserList),"users not checked.")

    # Call Botometer API for each user in distinctUserList
    botometerCheck = botometer_check_account(distinctUserList)
    res = await botometerCheck
    print("Finished Botometer API calls.")
    print("Botometer results for batch:",res)

    # Upsert documents in Cosmos DB with stats for each user returned by Botometer. 
    await upsertUserList(res)
    print("passed upsert")

    # Update daily counter
    await replace_item(botometerDailyCounterClient,botometerDailyCounter[0]['id'],botometerSessionRequestCount)

    # Update checkpoint and wait for the next event batch
    print("Finished upserting checked users in Cosmos DB, updating event hub checkpoint and waiting for next batch.")
    await partition_context.update_checkpoint()

if __name__ == '__main__':

    # Query Cosmosdb to check if a record for current day exists
    query="SELECT r.id as id, r.date AS date, r.checkCount AS checkCount FROM r WHERE r.date =" + '"' + datetime.now().strftime("%d/%m/%Y") + '"'
    botometerDailyCounter = list(botometerDailyCounterClient.query_items(query=query,enable_cross_partition_query=True))

    # If current day record exists, proceed to check whether the record count is < 1800
    # Else, create current day record and proceed
    if len(botometerDailyCounter) == 1:
        print("Daily counter already exists")
        botometerSessionRequestCount = botometerDailyCounter[0]['checkCount']
    elif len(botometerDailyCounter) > 1:
        print("Duplicate daily counter found, exiting BotometerChecker.")
        sys.exit()
    else:
        print("No daily counter found, creating...")
        botometerSessionRequestCount = 0
        botometerDailyCounter = [{"checkCount": 0}]
        create_item(botometerDailyCounterClient, datetime.now().strftime("%d/%m/%Y"),botometerSessionRequestCount)

    if int(botometerDailyCounter[0]['checkCount']) < int(botometer_session_limit) or int(botometerSessionRequestCount) < int(botometer_session_limit):
        print("Checks remaining for day:",int(botometer_session_limit) - int(botometerDailyCounter[0]['checkCount']))
    else:
        print("maximum checks done for the day, exiting BotometerChecker.")
        sys.exit()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(receive_batch())