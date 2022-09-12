import logging
import uuid
from datetime import date, timedelta, datetime

import pandas as pd
from bs4 import BeautifulSoup

from scraper import PipelineScraper

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class Williams(PipelineScraper):
    tsp = None
    tsp_name = None
    source_extensions = ['discovery', 'blackmarlin']
    source = 'williams'
    base_url = 'https://{}.williams.com'
    post_url = 'https://{}.williams.com/oa_detail.jsp'
    download_csv_url = 'https://{}.williams.com/oa_detail.jsp'

    def __init__(self, job_id=None):
        PipelineScraper.__init__(self, job_id, web_url=self.base_url, source=self.source)

    def start_scraping(self, post_date: date = None, cycle: int = 2):
        post_date = post_date if post_date is not None else date.today()

        main_df = pd.DataFrame()
        for extension in self.source_extensions:
            try:
                query_params = [
                    ('id', cycle),
                    ('nomDate', post_date.strftime('%m-%d-%Y'))
                ]
                logger.info('Scraping %s pipeline gas for post date: %s', self.source, post_date)
                response = self.session.get(self.post_url.format(extension), params=query_params)
                response.raise_for_status()

                soup = BeautifulSoup(response.content, 'lxml')

                table = soup.select_one('table.sortable')
                df_result = pd.read_html(str(table))[0]

                tsp_name = soup.select_one('title').text
                effective_date = soup.find_all("b", string="Effective Date:")[0].findNext("td").text
                posting_date = soup.find_all("b", string="Posting Date:")[0].findNext("td").text

                # couldn't find TSP ID on the website.
                df_result.insert(0, 'TSP Name', tsp_name, allow_duplicates=True)
                df_result.insert(1, 'Post Date/Time', posting_date, allow_duplicates=True)
                df_result.insert(2, 'Effective Gas Day/Time', effective_date, allow_duplicates=True)
                df_result.insert(3, 'Meas Basis Desc', 'MMBTU')  # All values are MMBTU mentioned static on website.

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
    query_date = datetime.fromisoformat("2022-08-11")
    scraper = Williams(job_id=str(uuid.uuid4()))

    # cycle = 1 - Timely
    # cycle = 2 - Evening (Default value passed)
    # cycle = 3 - Intraday 1
    # cycle = 4 - Intraday 2
    # cycle = 5 - Intraday 3

    scraper.start_scraping(post_date=query_date, cycle=2)
    scraper.scraper_info()


if __name__ == '__main__':
    main()