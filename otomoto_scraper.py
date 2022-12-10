import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen
import ssl 
import certifi
import regex as re
from csv import writer
import pandas as pd
import os

def find_unit(text):
    i = len(text)-1
    while(text[i] != ' '):
        i=i-1
    return text[:i] ,text[i:] 


def number_of_pages_oto(pre_url):
    r = urlopen(pre_url, context=ssl.create_default_context(cafile=certifi.where()))
    soup = BeautifulSoup(r, "html.parser")

    content = soup.find_all('li')
    page_pattern = re.compile('(?<=<li aria-label=\"Page )(.*?)(?=\" class=\"pagination)')
    page = re.findall(page_pattern,str(soup))
    if not page:
        number_of_pages = 1
    else:
        number_of_pages = page.pop()
    
    return number_of_pages

def scrape_content(pre_url,file_name,path):
    make_model_name = 0
    num_of_pages = int(number_of_pages_oto(pre_url))
    file_name_path = path + '\\'+ file_name + '.csv'

    for page in range(1,num_of_pages+1):
        url = "{}?page={}".format(pre_url,page)
        r = urlopen(url, context=ssl.create_default_context(cafile=certifi.where()))
        soup = BeautifulSoup(r, "html.parser")
        content = soup.find_all('article')


        if make_model_name == 0:
            model_pattern = re.compile('(?<=https:\/\/www\.otomoto\.pl\/osobowe\/)(.*?)(?=\?page)')
            model_name = re.search(model_pattern,url)
            model_name = str(model_name.group(0))
            model_name = model_name.replace(r'/','')
            model_name = model_name.replace(file_name,'')
            make_model_name = 1

      
        headerList = ['offer_name','model','brand', 'production_date','price','currency', 'mileage',
        'mileage_unit', 'engine_capacity','engine_capacity_unit', 'fuel_type']

        for offer in content:
            offer = str(offer)
            if "ooa-biizpf eodr1qw0" in offer:
                break
            
            offer_name_pattern = re.compile('(?<=target="_self">)(.*?)(?=<\/a>)')
            offer_name = re.search(offer_name_pattern,offer)
            offer_name = str(offer_name.group(0))

            offer_tech_pattern = re.compile('(?<=<li class="ooa-1k7nwcr e1teo0cs0">)(.*?)(?=<\/li>)')
            offer_tech_raw = re.findall(offer_tech_pattern,offer)

            offer_tech = []
            for specs in offer_tech_raw:
                if specs not in offer_tech and specs.lower() != "niski przebieg":
                    offer_tech.append(specs)
            
            try:
                if len(offer_tech) != 4:
                    offer_tech.append("0 km")
                    offer_tech[1],offer_tech[2]= offer_tech[2],offer_tech[1]
                    offer_tech[1],offer_tech[3]= offer_tech[3],offer_tech[1]
            except IndexError:
                continue
            
            price_pattern = re.compile('(?<=<span class="ooa-1bmnxg7 e1b25f6f11">)(.*?)(?=<\/span>)')
            price = re.search(price_pattern,offer)
            price = str(price.group(0))

            price, currency = find_unit(price)
            mileage, mileage_unit = find_unit(offer_tech[1])
            engine_capacity, engine_capacity_unit = find_unit(offer_tech[2])

            data = {'offer_name': [offer_name], 
                    'brand' : [file_name],
                    'model' : [model_name],
                    'production_date' : [offer_tech[0]],
                    'price' : [price],
                    'currency':[currency],
                    'mileage' : [mileage],
                    'mileage_unit' : [mileage_unit],
                    'engine_capacity' : [engine_capacity],
                    'engine_capacity_unit': [engine_capacity_unit],
                    'fuel_type':[offer_tech[3]]
                    }

            df = pd.DataFrame(data)
            df.to_csv( file_name_path, mode='a', index=False, header=False)



def srape_from_file():
    df = pd.read_csv('links_to_offers_otomoto.csv', sep=";",dtype=str)
    link_list = df.iloc[:, 0].tolist()

    headerList = ['offer_name','brand','model', 'production_date','price','currency', 'mileage',
                 'mileage_unit', 'engine_capacity','engine_capacity_unit', 'fuel_type']    
    cwd = os.getcwd()
    brand = ' '
    path = ' '
    count_rows = 0
    for link in link_list:
        count_rows += 1
        print(count_rows)
        if brand in link:
            scrape_content(link,brand,path)
            
        else:
            brand = re.compile('(?<=https:\/\/www\.otomoto\.pl\/osobowe\/)(.*?)(?=\/)')
            brand = re.search(brand,link)
            brand = str(brand.group(0))
            path = os.path.join(cwd, 'brands')            
            scrape_content(link,brand,path)
            
        try:
            file_name_path = path + '\\'+ brand + '.csv'
            file = pd.read_csv(file_name_path)
            file.to_csv(file_name_path, header=headerList, index=False)
        except FileNotFoundError:
            pass             
  


if __name__ == '__main__':
    srape_from_file()