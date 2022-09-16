import uuid
from datetime import date, timedelta
from io import StringIO
import logging

import pandas as pd
from bs4 import BeautifulSoup

from scraper import PipelineScraper


logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class EnergyTransfer(PipelineScraper):
    parent_source = 'energytransfer'
    tsp_list = [
        {
            'source': 'feptransfer.energytransfer',
            'home_url': 'https://feptransfer.energytransfer.com/ipost/FEP/main/index',
            'post_url': 'https://feptransfer.energytransfer.com/ipost/FEP/capacity/operationally-available',
            'download_api': 'https://feptransfer.energytransfer.com/ipost/capacity/operationally-available',
            'tsp_id': '829416002',
            'tsp_name': 'Fayetteville Express Pipeline, LLC',
            'asset': 'FEP'
        },
        {
            'source': 'feptransfer.energytransfer',
            'home_url': 'https://feptransfer.energytransfer.com/ipost/TW/main/index',
            'post_url': 'https://feptransfer.energytransfer.com/ipost/TW/capacity/operationally-available',
            'download_api': 'https://feptransfer.energytransfer.com/ipost/capacity/operationally-available',
            'tsp_id': '007933047',
            'tsp_name': 'Transwestern Pipeline Company, LLC',
            'asset': 'TW'
        },
        {
            'source': 'feptransfer.energytransfer',
            'home_url': 'https://feptransfer.energytransfer.com/ipost/TGR/main/index',
            'post_url': 'https://feptransfer.energytransfer.com/ipost/TGR/capacity/operationally-available',
            'download_api': 'https://feptransfer.energytransfer.com/ipost/capacity/operationally-available',
            'tsp_id': '829521983',
            'tsp_name': 'ETC Tiger Pipeline, LLC',
            'asset': 'TGR'
        }
    ]

    get_page_headers = {
        'Accept': 'text / html, application / xhtml + xml, application / xml; q = 0.9, image / avif, image / webp, image / apng, * / *;q = 0.8, application / signed - exchange; v = b3; q = 0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US, en; q = 0.9, fil; q = 0.8',
        'Connection': 'keep-alive',
        'Host': 'feptransfer.energytransfer.com',
        'Referer': 'https://feptransfer.energytransfer.com/ipost/FEP/capacity/operationally-available?max=ALL',
        'sec-ch-ua': '"Chromium";v="104","Not A;Brand";v="99","Google Chrome";v="104"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'
    }

    post_page_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,fil;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Length': '82',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'feptransfer.energytransfer.com',
        'Origin': 'https://feptransfer.energytransfer.com',
        'Referer': 'https://feptransfer.energytransfer.com/ipost/FEP/capacity/operationally-available?max=10',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }

    params = {
        'f': 'csv',
        'extension': 'csv',
        'searchType': 'NOM',
        'searchString': '',
        'locType': 'ALL',
        'locZone': 'ALL'
    }

    payload = {
        'searchType': 'NOM',
        'searchString': '',
        'locType': 'ALL',
        'locZone': 'ALL'
    }

    def __init__(self, job_id):
        PipelineScraper.__init__(self, job_id, web_url=self.tsp_list[0]['home_url'], source=self.parent_source)

    def set_params(self, post_date: date = None, cycle: int = 0):
        if post_date is None:
            set_date = {'gasDay': date.today().strftime('%m/%d/%Y')}
        else:
            set_date = {'gasDay': post_date.strftime('%m/%d/%Y')}
        self.params.update(set_date)

        set_cycle = {'cycle': str(cycle)}
        self.params.update(set_cycle)

        return self.params

    def set_payload(self, post_date: date = None, cycle: int = 0):
        if post_date is None:
            set_date = {'gasDay': date.today().strftime('%m/%d/%Y')}
        else:
            set_date = {'gasDay': post_date.strftime('%m/%d/%Y')}
        self.payload.update(set_date)

        set_cycle = {'cycle': str(cycle)}
        self.payload.update(set_cycle)

        return self.payload

    def add_columns(self, df_data, tsp, post_date: date = None, cycle: int = 0):
        payload = self.set_payload(post_date, cycle)
        page_response = self.session.post(tsp['post_url'], headers=self.post_page_headers, data=payload)
        soup = BeautifulSoup(page_response.text, 'lxml')

        # text is not enclosed by tags, the closest identifier is class:pad.
        post_date_time = soup.find_all('p', {'class': 'pad'})[0].findChild('strong').nextSibling.text
        eff_gas_day_time = soup.find_all('p', {'class': 'pad'})[1].findChild('strong').nextSibling.text
        meas_basis = soup.find_all('p', {'class': 'pad'})[2].findChild('strong').nextSibling.text

        df_data.insert(0, 'TSP', tsp['tsp_id'], True)
        df_data.insert(1, 'TSP Name', tsp['tsp_name'], True)
        df_data.insert(2, 'Post Date/Time', post_date_time.strip(), True)
        df_data.insert(3, 'Effective Gas Day/Time', eff_gas_day_time.strip(), True)
        df_data.insert(4, 'Meas Basis Desc', meas_basis.strip(), True)

        return df_data

    def start_scraping(self, post_date: date = None, cycle: int = 0):
        main_df = pd.DataFrame()
        for tsp in self.tsp_list:
            try:
                logger.info('Scraping %s pipeline gas for post date: %s', self.source, post_date)
                company = {'asset': tsp['asset']}
                self.params.update(company)
                params = self.set_params(post_date, cycle)

                response = self.session.get(tsp['download_api'], headers=self.get_page_headers, params=params)
                response.raise_for_status()

                html_text = response.text
                csv_data = StringIO(html_text)

                df_result = pd.read_csv(csv_data)
                final_report = self.add_columns(df_result, tsp, post_date, cycle)
                main_df = pd.concat([main_df, final_report])
                logger.info('DF created for %s', tsp['asset'])

            except Exception as ex:
                logger.error(ex, exc_info=True)

        self.save_result(main_df, post_date=post_date, local_file=True)
        logger.info('File saved. end of scraping: %s', self.parent_source)

        return None


def back_fill_pipeline_date():
    scraper = EnergyTransfer(job_id=str(uuid.uuid4()))
    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        print(post_date)
        scraper.start_scraping(post_date)


def main():
    # set your own date to scrape. default is current date
    custom_date = date.fromisoformat('2022-08-30')
    # set desired cycle:
    # 0 = Timely (Default)
    # 1 = Evening,
    # 3 = ID1,
    # 4 = ID2,
    # 7 = ID3.
    # 5 = Final - csv file might be empty if not yet available.
    custom_cycle = 1
    scraper = EnergyTransfer(job_id=str(uuid.uuid4()))
    scraper.start_scraping(post_date=custom_date, cycle=custom_cycle)
    scraper.scraper_info()


if __name__ == '__main__':
    main()
    