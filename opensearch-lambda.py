import boto3
import json
import requests
from requests_aws4auth import AWS4Auth

region = 'eu-west-2' # For example, us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

host = 'https://search-mss-no-vpc-lyfvmmsfghlxqziokdcg25mmu4.eu-west-2.es.amazonaws.com' # The OpenSearch domain endpoint with https://
index = 'lambda-iot-core-index'
url = host + '/' + index + '/_search'

# Lambda execution starts here
def lambda_handler(event, context):

    # Put the user query into the query DSL for more accurate search results.
    query = {
        "query": {
            "multi_match": {
                "query": event['queryStringParameters']['q']
                }
        }
    }
    
    # Elasticsearch 6.x requires an explicit Content-Type header
    headers = {"Content-Type": "application/json"}

    # Make the signed HTTP request
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))

    reqJson = r.json()

    trimmedData = trimData(reqJson)

    path = event['path']

    #modify this part for future endpoint 
    if path == '/energy-consumption':
        aggregate = aggregateConsumption(trimmedData)
        consumption = calculateConsumption(aggregate)
        
        result = {'consumption': consumption}
    
    else:
        result = trimmedData
    
    # Create the response and add some extra content to support CORS
    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": '*'
        },
        "isBase64Encoded": False
    }

    # Add the search results to the response
    response['body'] = json.dumps(result)
    return response

def trimData(data):
    trimmedData = []
    leafHits = {}

    try:
        leafHits = data['hits']['hits']
        for hit in leafHits:
            d = {'timestamp': hit['_source']['@timestamp'],'event': hit['_source']['event']}
            trimmedData.append(d)
    except KeyError:
        return 'No [hits]'

    return trimmedData

def aggregateConsumption(rawData):
    total = 0
    for d in rawData:
        total = total +d['event']['payload'][0]['metric']
    
    return total

def calculateConsumption(consumption):
    tarrif = 0.002
    return consumption * tarrif


