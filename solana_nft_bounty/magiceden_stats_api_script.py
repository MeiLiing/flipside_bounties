import requests
import json
import time

def meapi_collections_floor(offset=0, offset_range=500, offset_interval=500, call_limit=500, return_object=False):
    collections = []

    for n in range(offset,offset_range,offset_interval):
        print('STARTED collections offset: ',n)

        offset = str(n)
        url = "http://api-mainnet.magiceden.dev/v2/collections?offset="+offset+"&limit=500"
        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)
        response_collections_json = response.json()

        for i in range(0,len(response_collections_json)):
            symbol = response_collections_json[i]['symbol']

            print('STARTED inner collection (collections/inner offset): ',n,'/',i)
            print('Symbol: ',symbol)
            
            url = "http://api-mainnet.magiceden.dev/v2/collections/"+symbol+"/stats"
            payload = {}
            headers = {}
            response = requests.request("GET", url, headers=headers, data=payload)
            response_symbol_json = response.json()
            print('response_symbol_json complete')

            url = "http://api-mainnet.magiceden.dev/v2/collections/"+symbol+"/listings?offset=0&limit=1"
            payload = {}
            headers = {}
            response_listing = requests.request("GET", url, headers=headers, data=payload)
            response_listing_json = response_listing.json()
            print('response_listing_json complete')
            
            if len(response_listing_json) > 0:
                response_symbol_json['mint'] = response_listing_json[0]['tokenMint']
            else:
                response_symbol_json['mint'] = '0'

            collections += [response_symbol_json]

            print('ENDED inner collection: ',n,'/',i)
            print('Symbol: ',symbol)
            # print(collections)
            time.sleep(0.001)
        
        print('ENDED collections offset: ',n)

    print('API call loop complete.')

    print('Exporting to JSON.')

    json_export = 'magiceden_stats_export.json'
    with open(json_export,'w') as outfile:
        json.dump(collections, outfile)

    print('Export complete to: ',json_export)

    if return_object:
        print('Returned response object.')
        return collections
    else:
        pass
    
    print('Function ended.')