Program scrapes data from otomoto offers and save it into csv file. It uses BeautifulSoup, urlopen and regex to do so.

1 row contains those headers -  'brand','model', 'production_date','price','currency', 'mileage','mileage_unit', 'engine_capacity','engine_capacity_unit', 'fuel_type', 'batch_date'

Scraper to work needs link which contains brand and model of given car - https://www.otomoto.pl/osobowe/bmw/ .

Those are provided by correct_link.py script witch task is to scrape links from otomoto and save in correct_links.csv

Main pourpuse of correct_link.py is to automate proces of scraping links. Currently it can get around 650 models from 32 manufactuers(links to those are in link_source_code.csv)

link_source_code.csv -> correct_link.py -> correct_links.csv -> scraper.py -> otomoto_scraped_{date}.csv

I have included also link_decoder.py script whitch can, using regex generate all of models avaible in otomoto, but i found that some links of links were incorect so I leave it here as fun fact ;)

After running the script, data need to be further validetad, usualy becouse of duplicates, that been showed due to paid promotion. Colums can have also empty values, or be incorrect(date in brand colums ect.), all of those rows must be deleted, I use excel sheet to do so. If counting on otomoto site is correct scraper have around 85% efficacy.

