import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen
import ssl 
import certifi
import regex as re
from csv import writer
import pandas as pd
import os
import multiprocessing
import datetime
from tqdm import tqdm
from multiprocessing.pool import ThreadPool    


df = pd.read_csv('link_source_code.csv', sep=";",dtype=str,encoding="ISO-8859-1")


otomoto_link_list = df.otomoto_link.tolist()

for otomoto_link in otomoto_link_list:
    
    url = otomoto_link
    r = urlopen(url, context=ssl.create_default_context(cafile=certifi.where()))
    soup = BeautifulSoup(r, "html.parser")

    content = str(soup.find_all('div', class_ = "ooa-g7sbfj e6e1asl0"))

    link_pattern = re.compile('(?<=er34gjf0\" href=\")(.*?)(?=\" title=\")')
    link_list = re.findall(link_pattern,content)

    links = []
    for link in link_list:
        if 'mazowieckie' in link or 'slaskie' in link or 'wielkopolskie' in link or 'malopolskie' in link:
            break
        link = link[:len(link)-1]
        df = pd.DataFrame({link})
        df.to_csv('correct_links.csv', mode='a', index=False, header=False)


