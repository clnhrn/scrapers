import logging
import uuid
from datetime import date, timedelta, datetime

import pandas as pd
from bs4 import BeautifulSoup

from scraper import PipelineScraper

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class Williams(PipelineScraper):
    parent_source = 'williams'
    tsp_list = [
        {
            'source': 'discovery.williams',
            'home_url': 'https://discovery.williams.com',
            'download_api': 'https://discovery.williams.com/oa_detail.jsp',
            # can't find tsp id
            'tsp_id': '',
            'tsp_name': 'Discovery Gas Transmission LLC'
        },
        {
            'source': 'blackmarlin.williams',
            'home_url': 'https://blackmarlin.williams.com',
            'download_api': 'https://blackmarlin.williams.com/oa_detail.jsp',
            # can't find tsp id
            'tsp_id': '',
            'tsp_name': 'Black Marlin Pipeline Company'
        }]

    def __init__(self, job_id=None):
        PipelineScraper.__init__(self, job_id, web_url=self.tsp_list[0]['home_url'], source=self.parent_source)

    def start_scraping(self, post_date: date = None, cycle: int = 2):
        post_date = post_date if post_date is not None else date.today()

        main_df = pd.DataFrame()
        for tsp in self.tsp_list:
            try:
                query_params = [
                    ('id', cycle),
                    ('nomDate', post_date.strftime('%m-%d-%Y'))
                ]
                logger.info('Scraping %s pipeline gas for post date: %s', tsp['source'], post_date)
                response = self.session.get(tsp['download_api'], params=query_params)
                response.raise_for_status()

                soup = BeautifulSoup(response.content, 'lxml')

                table = soup.select_one('table.sortable')
                df_result = pd.read_html(str(table))[0]

                effective_date = soup.find_all("b", string="Effective Date:")[0].findNext("td").text
                posting_date = soup.find_all("b", string="Posting Date:")[0].findNext("td").text

                df_result.insert(0, 'TSP Name', tsp['tsp_name'], allow_duplicates=True)
                df_result.insert(1, 'Post Date/Time', posting_date, allow_duplicates=True)
                df_result.insert(2, 'Effective Gas Day/Time', effective_date, allow_duplicates=True)
                # All values are MMBTU mentioned static on website.
                df_result.insert(3, 'Meas Basis Desc', 'MMBTU')

                main_df = pd.concat([main_df, df_result])
            except Exception as ex:
                logger.error(ex, exc_info=True)

        logger.info('File saved. end of scraping: %s', self.source)
        self.save_result(main_df, post_date=post_date, local_file=True)

        return None


def back_fill_pipeline_date():
    scraper = Williams(job_id=str(uuid.uuid4()))
    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        print(post_date)
        scraper.start_scraping(post_date)


def main():
    query_date = datetime.fromisoformat("2022-09-13")
    scraper = Williams(job_id=str(uuid.uuid4()))

    # cycle = 1 - Timely
    # cycle = 2 - Evening (Default value passed)
    # cycle = 3 - Intraday 1
    # cycle = 4 - Intraday 2
    # cycle = 5 - Intraday 3

    scraper.start_scraping(post_date=query_date, cycle=5)
    scraper.scraper_info()


if __name__ == '__main__':
    main()
