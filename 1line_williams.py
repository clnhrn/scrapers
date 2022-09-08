import uuid
from datetime import date, timedelta
from io import StringIO
import logging

import pandas as pd

from scraper import PipelineScraper

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__) 


class OneLineWilliams(PipelineScraper):
    source_extensions_main = ['pineneedle', '1line', '1line']
    source_extensions_mid = ['williams', 'williams', 'gulfstreamgas']
    source_extensions_end = ['PineNeedle', 'Transco', 'GulfStream']
    bu_id = ['82', '80', '205']
    source = '1line.williams'
    api_url = "https://www.{}.{}.com/{}/index.html"
    post_data_url = "https://www.{}.{}.com/ebbCode/OACQueryRequest.jsp?BUID={}&type=OAC"
    get_data_url = 'https://www.{}.{}.com/ebbCode/OACreport.jsp'
    download_data_url = 'https://www.{}.{}.com/ebbCode/OACreportCSV.jsp'

    post_page_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Content-Length': '184',
        'Content-Type': 'application/x-www-form-urlencoded',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Dest': 'iframe',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'
    }

    get_page_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'
    }

    def __init__(self, job_id):
        PipelineScraper.__init__(self, job_id, web_url=self.api_url, source=self.source)

    def update_post_headers(self, main_ext, mid_ext, bu_id):
        post_headers = {
            'Host': 'www.{}.{}.com'.format(main_ext, mid_ext),
            'Origin': 'https://www.{}.{}.com'.format(main_ext, mid_ext),
            'Referer': 'https://www.{}.{}.com/ebbCode/OACQueryRequest.jsp?BUID={}'.format(main_ext, mid_ext, bu_id)
        }
        self.post_page_headers.update(post_headers)

        return self.post_page_headers

    def update_get_headers(self, main_ext, mid_ext, bu_id):
        get_headers = {
            'Host': 'www.{}.{}.com'.format(main_ext, mid_ext),
            'Referer': 'https://www.{}.{}.com/ebbCode/OACQueryRequest.jsp?BUID={}'.format(main_ext, mid_ext, bu_id)
        }
        self.get_page_headers.update(get_headers)

        return self.get_page_headers

    def get_payload(self, cycle: int = None, post_date: date = None):
        payload = {
            'MapID': '0',
            'submitflag': 'true',
            'recordCount': '550',
            'recordLimit': '52000',
            'SSDStartDate': '06/15/2011',
            'tbGasFlowBeginDate': post_date.strftime('%m/%d/%Y'),
            'tbGasFlowEndDate': post_date.strftime('%m/%d/%Y'),
            'cycle': cycle,
            'locationIDs': '',
            'reportType': '',
        }

        return payload

    def start_scraping(self, cycle: int = None, post_date: date = None):
        post_date = post_date if post_date is not None else date.today()
        cycle = cycle if cycle is not None else 1

        main_df = pd.DataFrame()
        for main_site, mid, end, b_id in zip(self.source_extensions_main, self.source_extensions_mid, self.source_extensions_end, self.bu_id):
            try:
                logger.info('Scraping %s pipeline gas for post date: %s', self.source, post_date)
                payload = self.get_payload(cycle=cycle, post_date=post_date)
                response = self.session.post(self.post_data_url.format(main_site, mid, b_id), data=payload, headers=self.update_post_headers(main_site, mid, b_id))
                response.raise_for_status()

                response = self.session.get(self.get_data_url.format(main_site, mid), headers=self.update_get_headers(main_site, mid, b_id))
                response.raise_for_status()

                response = self.session.get(self.download_data_url.format(main_site, mid), headers=self.update_get_headers(main_site, mid, b_id))
                html_text = response.text

                csv_data = StringIO(html_text)
                df_result = pd.read_csv(csv_data)
                main_df = pd.concat([main_df, df_result])
                logger.info('File saved. end of scraping: %s', self.source)
            except Exception as ex:
                logger.error(ex, exc_info=True)

        self.save_result(main_df, post_date=post_date, local_file=True)

        return None


def back_fill_pipeline_date():
    scraper = OneLineWilliams(job_id=str(uuid.uuid4()))
    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        print(post_date)
        scraper.start_scraping(post_date)


def main():
    scraper = OneLineWilliams(job_id=str(uuid.uuid4()))
    custom_date = date.fromisoformat('2022-08-15')
    # Cycles: Timely = 1, Evening = 2, Intra Day 1 = 3, Intra Day 2 = 4, Intra Day 3 = 8
    set_cycle = 2
    scraper.start_scraping(post_date=custom_date, cycle=set_cycle)


if __name__ == '__main__':
    main()
