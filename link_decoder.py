import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen
import ssl 
import certifi
import regex as re
import pandas as pd
from io import StringIO

df = pd.read_csv('link_source_code.csv', sep=";",dtype=str,encoding="ISO-8859-1")

cource_code_list = df.source_code.tolist()
otomoto_link_list = df.otomoto_link.tolist()


for cource_code in cource_code_list:
    cur_link = otomoto_link_list[0]
    otomoto_link_list.pop(0)

    model_pattern = re.compile('(?<=dropdown-item-text\" class=\"ooa-1mq1qia\">)(.*?)(?= \()')
    model_list = re.findall(model_pattern,cource_code)

    for model in model_list:
        model = model.lower()
        model = model.replace(' ','-')
        link_to_model_offers = cur_link + r'/' + model
        if 'nie-wybrano' in model:
            continue
        print(link_to_model_offers)
        df = pd.DataFrame({link_to_model_offers})
        df.to_csv('links_to_offers_otomoto.csv', mode='a', index=False, header=False)
    