import pandas as pd
import json
import datetime
import os
import cloudscraper
import asyncio
import aiohttp
from rich.console import Console
from dotenv import load_dotenv

load_dotenv()
proxy = os.getenv('PROXY')

proxy = ''



values_to_push_root = ['one_day_volume', 'one_day_change', 'one_day_sales', 'one_day_average_price',
                                           'seven_day_volume', 'seven_day_change', 'seven_day_sales',
                                           'seven_day_average_price', 'thirty_day_volume', 'thirty_day_change',
                                           'thirty_day_sales', 'thirty_day_average_price', 'total_volume',
                                           'total_sales', 'total_supply', 'count', 'num_owners', 'average_price',
                                           'num_reports', 'market_cap', 'floor_price']

async def scrape_collection_data_async_proxy_aiohttp_list(limit=300, offset=0, proxy=None):
    url = 'https://api.opensea.io/api/v1/collections?format=json&limit={}&offset={}'.format(limit, offset)
    attempts_max = 5
    concurrent_attempts = 0
    while concurrent_attempts < attempts_max:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, proxy=proxy) as response:
                    data = await response.json()
                    data_clean = []
                    for collection in data['collections']:
                        data_clean.append({'slug': collection['slug'], 'name': collection['name']})
                    break
        except Exception as e:
            concurrent_attempts += 1
            await asyncio.sleep(1)
    # if failed to connect to API, return empty dataframe
    if concurrent_attempts == attempts_max:
        return pd.DataFrame()
    return pd.DataFrame(data_clean)


async def scrape_collection_data_async_proxy_aiohttp_single(slug, proxy=None):
    url = 'https://api.opensea.io/api/v1/collection/{}/stats?format=json'.format(slug)
    attempts_max = 5
    concurrent_attempts = 0
    data_clean = {}
    while concurrent_attempts < attempts_max:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, proxy=proxy) as response:
                    data = await response.json()

                    for val in values_to_push_root:
                        data_clean[val] = data['stats'][val]
                    data_clean['slug'] = slug
                    break
        except Exception as e:
            concurrent_attempts += 1
            # print("Failed to connect to API. Retrying...")
            # print(e)
            # print(offset)
            await asyncio.sleep(1)
    # if failed to connect to API, return empty dataframe
    if concurrent_attempts == attempts_max:
        return data
    return data_clean

df_full = pd.DataFrame()


console = Console()
offset = 0
batches_done = 0

while True:
    try:
        # make requests in parallel 10 threads
        threads = 10
        loop = asyncio.get_event_loop()
        tasks = [asyncio.ensure_future(scrape_collection_data_async_proxy_aiohttp_list(offset=offset + 300 * i, proxy=proxy)) for i in range(threads)]
        loop.run_until_complete(asyncio.wait(tasks))

        # concatenate dataframes
        len_of_batch_results = 0
        for task in tasks:
            df_task = task.result()
            len_of_batch_results += len(df_task)
            if len(df_task) > 0:
                tasks_stats = [asyncio.ensure_future(scrape_collection_data_async_proxy_aiohttp_single(slug=collection['slug'], proxy=proxy)) for collection in df_task.to_dict('records')]
                loop.run_until_complete(asyncio.wait(tasks_stats))
                try:
                    for task_stats in tasks_stats:
                        stats = task_stats.result()
                        for val in values_to_push_root:
                            df_task.loc[df_task['slug'] == stats['slug'], val] = stats[val]
                except Exception as e:
                    print(e)
                    print(stats)
                    print(df_task)
                df_full = pd.concat([df_full, df_task])


        len_df_full = len(df_full)
        console.print(f"Scraped {len_of_batch_results} records, total {len_df_full} records", style="green")
        offset += 300 * threads
        batches_done += 1
        console.print(f"Batches done: {batches_done}", style="green")
        # print df memory usage
        print(f"Memory usage: {df_full.memory_usage().sum()/1024/1024} MB")

        if len_of_batch_results < 300:
            # save df to file
            df_full.to_csv("collections.csv", index=False)
            break
    except KeyboardInterrupt:
        df_full.to_csv("collections.csv", index=False)
        break



# scrape https://opensea.io/rankings?sortBy=total_volume with cloudscraper
# and return a list of dicts
def get_opensea_trending(unique_slugs, url):
    html = scraper.get(url).text
    json_string = html.split("</script>", 2)[0].split("window.__wired__=", 2)[1]
    data = json.loads(json_string)
    data_values = data["records"].values()
    data_list = [*data_values]
    list_of_dicts = []
    for key in data_list:
        if 'slug' in key:
            if key['slug'] not in unique_slugs:
                unique_slugs.append(key['slug'])
                list_of_dicts.append({'slug': key['slug'], 'name': key['name']})


    return list_of_dicts, unique_slugs

url_list = ['https://opensea.io/rankings?sortBy=one_day_volume', 'https://opensea.io/rankings?sortBy=seven_day_volume', 'https://opensea.io/rankings?sortBy=thirty_day_volume', 'https://opensea.io/rankings?sortBy=total_volume']
#create session
scraper  = cloudscraper.create_scraper()
#unique = cloudscraper.create_scraper(scraper)
# scrape all urls
unique_slugs = []
big_list = []
for url in url_list:
    list_scraped , unique_slugs = get_opensea_trending(unique_slugs, url)
    big_list.extend(list_scraped)

df2 = pd.DataFrame()
print(len(big_list))
df2 = pd.DataFrame(big_list)

loop = asyncio.get_event_loop()
tasks_stats = [
    asyncio.ensure_future(scrape_collection_data_async_proxy_aiohttp_single(slug=collection['slug'], proxy=proxy)) for
    collection in big_list]
loop.run_until_complete(asyncio.wait(tasks_stats))
for task_stats in tasks_stats:
    stats = task_stats.result()
    for val in values_to_push_root:
        df2.loc[df2['slug'] == stats['slug'], val] = stats[val]

df_full = pd.concat([df_full, df2])
df_full = df_full.drop_duplicates(subset='slug', keep='first')
df_full['date'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
columns = ['date', 'slug', 'name', 'count', 'floor_price', 'market_cap', 'num_owners', 'num_reports', 'one_day_average_price', 'one_day_change',
           'one_day_sales', 'one_day_volume', 'seven_day_average_price', 'seven_day_change', 'seven_day_sales', 'seven_day_volume',
           'thirty_day_average_price', 'thirty_day_change', 'thirty_day_sales', 'thirty_day_volume', 'total_sales',
           'total_supply', 'total_volume']
# read in data from csv file and convert to dataframe (collections.csv'



convert_to_int = ['count', 'num_owners', 'num_reports', 'one_day_sales', 'seven_day_sales', 'thirty_day_sales',
                  'total_sales', 'total_supply', 'market_cap', 'one_day_volume', 'seven_day_volume']
# covert this columns to int fill 0 if empty
for column in convert_to_int:
    df_full[column] = df_full[column].fillna(0).astype(int)

# remove columns that are not is columns list
df_export = df_full[columns]


df_export.to_csv("collections.csv", index=False)