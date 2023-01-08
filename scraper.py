import os
from bs4 import BeautifulSoup
from urllib.request import urlopen
import ssl 
import certifi
import regex as re
from csv import writer
import pandas as pd
import datetime
from tqdm import tqdm
from multiprocessing.pool import ThreadPool

def find_unit(text):
    i = len(text)-1
    while(text[i] != ' '):
        i=i-1
    obj = text[:i]

    return obj ,text[i:] 


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


        
def scrape_multiprocesing(pre_url):
    brand_name = ' '
    path = ' '
    batch_date = datetime.datetime.now()
    batch_date = batch_date.strftime('%d.%m.%Y')
    batch_date_name = batch_date.replace('.','_')
    cwd = os.getcwd()
    filename = 'otomoto_scraped_data_' + batch_date_name + '.csv'

    brand_pattern = re.compile('(?<=https:\/\/www\.otomoto\.pl\/osobowe\/)(.*?)(?=\/)')
    brand_name = re.search(brand_pattern,pre_url)
    brand_name = str(brand_name.group(0))
    path = os.path.join(cwd, 'otomoto')  
    num_of_pages = int(number_of_pages_oto(pre_url))
 
    pre_url = pre_url + '?page'

    model_pattern = re.compile('(?<=https:\/\/www\.otomoto\.pl\/osobowe\/)(.*?)(?=\?page)')
    model_name = re.search(model_pattern,pre_url)
    model_name = str(model_name.group(0))
    model_name = model_name.replace(r'/','')
    model_name = model_name.replace(brand_name,'')
    
    file_name_path = path + '\\'+ filename
    for page in range(1,num_of_pages+1):
        url = "{}={}".format(pre_url,page)
        r = urlopen(url, context=ssl.create_default_context(cafile=certifi.where()))
        soup = BeautifulSoup(r, "html.parser")
        content = soup.find_all('article')
        production_date = ''
        offer_tech =[]
        for offer in content:
            offer = str(offer)
            if "ooa-biizpf eodr1qw0" in offer:
                break
                   
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

            price = int(price.replace(' ',''))
            mileage = int(mileage.replace(' ',''))
            engine_capacity = int(engine_capacity.replace(' ',''))
            
            production_date = offer_tech[0]
            
            data = {
                    'brand' : [brand_name],
                    'model' : [model_name],
                    'production_date' : [production_date],
                    'price' : [price],
                    'currency':[currency],
                    'mileage' : [mileage],
                    'mileage_unit' : [mileage_unit],
                    'engine_capacity' : [engine_capacity],
                    'engine_capacity_unit': [engine_capacity_unit],
                    'fuel_type':[offer_tech[3]],
                    'scrape_date':[batch_date]
                    }

            df = pd.DataFrame(data)
            df_to_file = pd.concat([df, df_to_file], axis=0)
            
    df_to_file.to_csv(file_name_path, mode='a', index=False, header=False)
        


def srape_from_file():
    df = pd.read_csv('correct_links.csv', sep=";",dtype=str)
    link_list = df.iloc[:, 0].tolist()

    headerList = ['batch_number','offer_name','brand','model', 'production_date','price','currency', 'mileage',
                 'mileage_unit', 'engine_capacity','engine_capacity_unit', 'fuel_type', 'batch_date']    

    pool = ThreadPool()
    for item in tqdm(link_list):
        pool.apply_async(scrape_multiprocesing, (item,))

    pool.close()
    pool.join()


if __name__ == '__main__':
    srape_from_file()