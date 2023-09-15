#!/usr/bin/env python
# coding: utf-8

# In[134]:


# Importing packages
import pandas as pd
import discogs_client
import re
import time
import requests
import json
import os
from dotenv import load_dotenv


# # Discogs

# In[135]:


# Client
load_dotenv()

discogs = discogs_client.Client('Discogs_to_Notion', user_token=os.getenv('discogs_token'))
me = discogs.identity()


# ## Release ids of Discogs wantlist and collection

# In[136]:


# Discogs wantlist
wantlist = me.wantlist
wantlist_ids = []
for item in wantlist:
    print(item)
    wantlist_ids.append(item.id)


# In[137]:


# Discogs collection
collection = me.collection_folders[0].releases
collection_ids = []
for item in collection:
    print(item)
    # Add release ids to list
    collection_ids.append(item.id)


# ## Get album info from Discogs

# In[138]:


def get_album_info(release_ids, tag):
    # Initialize empty lists
    albums = []
    artists = []
    urls = []
    images = []

    for i in release_ids:
        release = discogs.release(i)                                                    # initiate release object using id
        album_title = release.title                                                     # album title
        artist_name = release.artists                                                   # artist name
        artist_filter = re.sub(r"^\[<Artist \d+ '(.*)'>\]$", r"\1", str(artist_name))   # filter artist name from list
        artist_filter = re.sub(r"\d+", "", artist_filter)
        artist_filter = artist_filter.replace("()", "")
        album_url = release.url                                                         # url
        album_image = release.images                                                    # image
        album_image = album_image[0]
        album_image = album_image.get('uri')                                                                                      
        
        # Append info to lists
        albums.append(album_title)
        artists.append(artist_filter)
        urls.append(album_url)
        images.append(album_image)

        # Print to see progress
        print(release)
        time.sleep(0.5)

    # Store info in df
    df = pd.DataFrame(list(zip(albums, artists, urls, images)),
                                    columns = ['album', 'artist', 'url', 'image'])
    
    if tag == 'wish':
        df['tags'] = 'wish'
    elif tag == 'collection':
        df['tags'] = 'collection'
    else:
        df['tags'] = None
                                    
    return df


# In[139]:


discogs_collection = get_album_info(collection_ids, 'collection')
discogs_collection.head()


# In[140]:


discogs_wantlist = get_album_info(wantlist_ids, 'wish')
discogs_wantlist.head()


# # Notion

# In[141]:


# Initialization
token = os.getenv('notion_token')
database_id = '1afa86cc349c402ab660a19466400390'
headers = {
    'Authorization': 'Bearer ' + token,
    'Content-type': 'application/json',
    'Notion-Version': '2022-06-28'
}


# ## Get pages info (read)

# In[142]:


def get_pages():
    read_url = f'https://api.notion.com/v1/databases/{database_id}/query'

    response = requests.post(read_url, headers=headers)

    data = response.json()
    with open('db.json', 'w', encoding='utf8') as f:    
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    results = data['results']
    return results


# In[143]:


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

    df = pd.DataFrame(list(zip(albums, artists, urls, tags, images, page_ids)),
                                    columns = ['album', 'artist', 'url', 'tags', 'image', 'page_id'])
    
    return df


# In[144]:


notion_pages = get_pages_info(pages)
notion_pages.head()


# ## Delete old pages

# In[145]:


def delete_page(page_id: str):
    url = f"https://api.notion.com/v1/pages/{page_id}"

    payload = {"archived": True}

    res = requests.patch(url, json=payload, headers=headers)
    return res


# In[146]:


for index, row in notion_pages.iterrows():
    page_id = row['page_id']
    album = row['album']

    print(f'Deleting {album} page...')
    delete_page(page_id)


# ## Create new pages using Discogs info

# In[147]:


def create_page(data: dict):
    create_url = 'https://api.notion.com/v1/pages'

    payload = {'parent': {'database_id': database_id}, 'properties': data}

    res = requests.post(create_url, headers=headers, json=payload)
    print(res.status_code)
    
    return res


# In[148]:


def setup_data(df):
    for index, row in df.iterrows():
        # Extract data from df
        album = row['album']
        artist = row['artist']
        url = row['url']
        tag = row['tags']
        image = row['image']

        # Defining color for tags
        if tag == 'wish':
            color = 'purple'
        elif tag == 'collection':
            color = 'green'
        else:
            color = 'blue'

        # Setting up data for new page
        data = {
            'Album': {'title': [{'text': {'content': album}}]},
            'Artist': {'multi_select': [{'name': artist}]},
            'URL': {'url': url},
            'Tags': {'multi_select': [{'name': tag, 'color': color}]},
            'Album cover': {'files': [{'name': 'image', 'type': 'external', 'external': {'url': image}}]}
            }

        print(f'Creating {album} page...')
        create_page(data)


# In[149]:


# Setup data and create new pages
setup_data(discogs_collection)
setup_data(discogs_wantlist)

