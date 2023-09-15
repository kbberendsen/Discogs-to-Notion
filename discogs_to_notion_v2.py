# %%
# Importing packages
import pandas as pd
import discogs_client
import re
import time
import requests
import json
import os
import concurrent.futures
from dotenv import load_dotenv

# %% [markdown]
# # Discogs

# %%
# Client
load_dotenv()

discogs = discogs_client.Client('Discogs_to_Notion', user_token=os.getenv('discogs_token'))
me = discogs.identity()

# %% [markdown]
# ## Release ids of Discogs wantlist and collection

# %%
# Discogs wantlist
wantlist = me.wantlist
wantlist_ids = []
for item in wantlist:
    print(item)
    wantlist_ids.append(item.id)

# %%
# Discogs collection
collection = me.collection_folders[0].releases
collection_ids = []
for item in collection:
    print(item)
    # Add release ids to list
    collection_ids.append(item.id)

# %% [markdown]
# ## Get album info from Discogs

# %%
def get_album_info(release_ids, tag):
    # Create empty pandas dataframe with desired columns
    df = pd.DataFrame(columns=['album', 'artist', 'url', 'image'])

    def get_release_info(release_id):
        try:
            release = discogs.release(release_id)                                           # initiate release object using id
            album_title = release.title                                                     # album title
            artist_name = re.sub(r'\([^()]*\)', '', release.artists[0].name)                # artist name
            album_url = release.url                                                         # url
            album_image = release.images[0].get('uri')                                      # image

            # Add row to dataframe with album information
            df.loc[len(df)] = [album_title, artist_name, album_url, album_image]          

            return album_title, artist_name, album_url, album_image
        except:
            return None

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit requests for each release_id
        futures = [executor.submit(get_release_info, release_id) for release_id in release_ids]

        # Retrieve results as they become available
        for future in concurrent.futures.as_completed(futures):
            result = future.result()

            if result is not None:
                album_title, artist_name, album_url, album_image = result

                # Print to see progress
                print(album_title)
    
    tag_dict = {'wish': 'wish', 'collection': 'collection'}
    df['tags'] = tag_dict.get(tag, None)
                                    
    return df

# %%
discogs_collection = get_album_info(collection_ids, 'collection')
discogs_collection.head()

# %%
discogs_wantlist = get_album_info(wantlist_ids, 'wish')
discogs_wantlist.head()

# %% [markdown]
# # Notion

# %%
# Initialization
token = os.getenv('notion_token')
database_id = '1afa86cc349c402ab660a19466400390'
headers = {
    'Authorization': 'Bearer ' + token,
    'Content-type': 'application/json',
    'Notion-Version': '2022-06-28'
}

# %% [markdown]
# ## Get pages info (read)

# %%
def get_pages():
    read_url = f'https://api.notion.com/v1/databases/{database_id}/query'

    response = requests.post(read_url, headers=headers)

    data = response.json()
    with open('db.json', 'w', encoding='utf8') as f:    
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    results = data['results']
    return results

# %%
pages = get_pages()

def get_pages_info(pages):

    albums = []
    artists = []
    urls = []
    tags = []
    page_ids = []
    images = []

    for page in pages:
       
        page_id = page['id']
        props = page['properties']
        album_title = props['Album']['title'][0]['text']['content']
        artist = props['Artist']['multi_select'][0]['name']
        album_url = props['URL']['url']
        tag = props['Tags']['multi_select'][0]['name']
        image = props['Album cover']['files'][0]['external']['url']

        # Append info to lists
        albums.append(album_title)
        artists.append(artist)
        urls.append(album_url)
        tags.append(tag)
        images.append(image)
        page_ids.append(page_id)

    df = pd.DataFrame(zip(albums, artists, urls, tags, images, page_ids),
                                    columns = ['album', 'artist', 'url', 'tags', 'image', 'page_id'])
    
    return df

# %%
notion_pages = get_pages_info(pages)
notion_pages.head()

# %% [markdown]
# ## Delete old pages

# %%
# Function to delete multiple pages at once using multi-threading
def delete_pages(page_ids: list):

    # Function for the deletion of one single page
    def delete_page(page_id: str):
        url = f"https://api.notion.com/v1/pages/{page_id}"

        payload = {"archived": True}

        res = requests.patch(url, json=payload, headers=headers)
        return page_id, res

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(delete_page, page_id) for page_id in page_ids]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            # Print response code after each page deletion
            print(result)

# %%
# Get a list of page IDs to delete
page_ids = notion_pages['page_id'].tolist()

# Delete the pages using multi-threading
delete_pages(page_ids)

# %% [markdown]
# ## Create new pages using Discogs info

# %%
# Function to create multiple pages at once using multi-threading
def create_pages(data_list: list):

    # Function for the creation of one single page
    def create_page(data: dict):
        create_url = 'https://api.notion.com/v1/pages'

        payload = {'parent': {'database_id': database_id}, 'properties': data}

        res = requests.post(create_url, headers=headers, json=payload)
        
        # Retrieve album name
        album = data.get('Album')
        album = album['title'][0]['text']['content']

        # Return album name and corresponding response code
        return album, res

    # Multi-threading
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(create_page, data) for data in data_list]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            # Print response code after creating each page
            print(result)

# %%
def setup_data(df):
    # Define tag colours
    colours = {'wish': 'purple', 'collection': 'green'}

    # Define empty list for adding data dicts
    data_list = []

    for index, row in df.iterrows():
        # Extract data from df
        album = row['album']
        artist = row['artist']
        url = row['url']
        tag = row['tags']
        image = row['image']
        color = colours.get(tag, 'Blue')          # Define tag color based on tag value

        # Setting up data for new page
        data = {
            'Album': {'title': [{'text': {'content': album}}]},
            'Artist': {'multi_select': [{'name': artist}]},
            'URL': {'url': url},
            'Tags': {'multi_select': [{'name': tag, 'color': color}]},
            'Album cover': {'files': [{'name': 'image', 'type': 'external', 'external': {'url': image}}]}
            }

        data_list.append(data)
    
    # Create pages using multi-threading
    create_pages(data_list)

# %%
# Setup data and create new pages
setup_data(discogs_collection)
setup_data(discogs_wantlist)


