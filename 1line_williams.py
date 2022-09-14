import uuid
from datetime import date, timedelta
from io import StringIO
import logging

import pandas as pd

from scraper import PipelineScraper

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__) 


class OneLineWilliams(PipelineScraper):
    parent_source = 'williams'
    tsp_list = [
        {
            'source': 'pineneedle.williams',
            'home_url': 'https://www.pineneedle.williams.com/PineNeedle/index.html',
            'download_api': 'https://www.pineneedle.williams.com/ebbCode/OACreportCSV.jsp',
            'post_data_url': "https://www.pineneedle.williams.com/ebbCode/OACQueryRequest.jsp?BUID=82&type=OAC",
            'get_data_url': 'https://www.pineneedle.williams.com/ebbCode/OACreport.jsp',
            'bu_id': 82
        },
        {
            'source': '1line.williams',
            'home_url': 'https://www.1line.williams.com/Transco/index.html',
            'download_api': 'https://www.1line.williams.com/ebbCode/OACreportCSV.jsp',
            'post_data_url': "https://www.1line.williams.com/ebbCode/OACQueryRequest.jsp?BUID=80&type=OAC",
            'get_data_url': 'https://www.1line.williams.com/ebbCode/OACreport.jsp',
            'bu_id': 80
        },
        {
            'source': '1line.gulfstreamgas',
            'home_url': 'https://www.1line.gulfstreamgas.com/GulfStream/index.html',
            'download_api': 'https://www.1line.gulfstreamgas.com/ebbCode/OACreportCSV.jsp',
            'post_data_url': "https://www.1line.gulfstreamgas.com/ebbCode/OACQueryRequest.jsp?BUID=205&type=OAC",
            'get_data_url': 'https://www.1line.gulfstreamgas.com/ebbCode/OACreport.jsp',
            'bu_id': 205
        }
    ]

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
        PipelineScraper.__init__(self, job_id, web_url=self.tsp_list[0]['home_url'], source=self.parent_source)

    def update_post_headers(self, src, bu_id):
        post_headers = {
            'Host': 'www.{}.com'.format(src),
            'Origin': 'https://www.{}.com'.format(src),
            'Referer': 'https://www.{}.com/ebbCode/OACQueryRequest.jsp?BUID={}'.format(src, bu_id)
        }
        self.post_page_headers.update(post_headers)

        return self.post_page_headers

    def update_get_headers(self, src, bu_id):
        get_headers = {
            'Host': 'www.{}.com'.format(src),
            'Referer': 'https://www.{}.com/ebbCode/OACQueryRequest.jsp?BUID={}'.format(src, bu_id)
        }
        self.get_page_headers.update(get_headers)

        return self.get_page_headers

    def set_payload(self, post_date: date, cycle: int):
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

    def start_scraping(self, post_date: date = None, cycle: int = 1):
        post_date = post_date if post_date is not None else date.today()

        main_df = pd.DataFrame()
        for tsp in self.tsp_list:
            try:
                logger.info('Scraping %s pipeline gas for post date: %s', tsp['source'], post_date)
                payload = self.set_payload(cycle=cycle, post_date=post_date)
                response = self.session.post(tsp['post_data_url'], data=payload, headers=self.update_post_headers(tsp['source'], tsp['bu_id']))
                response.raise_for_status()

                response = self.session.get(tsp['get_data_url'], headers=self.update_get_headers(tsp['source'], tsp['bu_id']))
                response.raise_for_status()

                response = self.session.get(tsp['download_api'], headers=self.update_get_headers(tsp['source'], tsp['bu_id']))
                html_text = response.text

                csv_data = StringIO(html_text)
                df_result = pd.read_csv(csv_data)
                main_df = pd.concat([main_df, df_result])

            except Exception as ex:
                logger.error(ex, exc_info=True)

        logger.info('File saved. end of scraping: %s', self.parent_source)
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
    set_date = date.fromisoformat('2022-09-13')
    # Cycles:
    # 1 = Timely (Default)
    # 2 = Evening
    # 3 = Intra Day 1
    # 4 = Intra Day 2
    # 8 = Intra Day 3
    set_cycle = 2
    scraper.start_scraping(post_date=set_date, cycle=set_cycle)


if __name__ == '__main__':
    main()
