import uuid
from datetime import date, timedelta
from io import StringIO
import logging

import pandas as pd

from scraper import PipelineScraper

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class GasNom(PipelineScraper):
    source = 'gasnom'
    source_extensions = ['kinetica', 'perryville', 'cadeville', 'eastcheyenne', 'kde', 'spirestoragewest', 'leafriver', 'ugi']
    base_api_url = 'http://www.gasnom.com/ip/'
    post_data_url = 'http://www.gasnom.com/ip/{}/OAC.cfm'

    post_page_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': "keep-alive",
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'www.gasnom.com',
        'Origin': 'http://www.gasnom.com',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36v'
    }

    payload = {
        'qry': '1',
        'B1': 'Download'
    }

    def __init__(self, job_id):
        PipelineScraper.__init__(self, job_id, web_url=self.base_api_url, source=self.source)

    def get_payload(self, post_date: date = None):

        if post_date is None:
            dates = {'frmEffectiveDt': date.today().strftime('%m/%d/%Y'),
                     'FRMENDDT': date.today().strftime('%m/%d/%Y')}
        else:
            dates = {'frmEffectiveDt': post_date.strftime('%m/%d/%Y'),
                     'FRMENDDT': post_date.strftime('%m/%d/%Y')}

        self.payload.update(dates)

        return self.payload

    def start_scraping(self, post_date: date = None):
        """
        :param post_date:
        :return:
        """
        main_df = pd.DataFrame()
        for extension in self.source_extensions:
            try:
                logger.info('Scraping %s pipeline gas for post date: %s', self.source, post_date)
                payload = self.get_payload(post_date)
                response = self.session.post(self.post_data_url.format(extension), data=payload,
                                             headers=self.post_page_headers)
                response.raise_for_status()

                html_text = response.text

                csv_data = StringIO(html_text)
                df_result = pd.read_csv(csv_data, sep='\t')
                main_df = pd.concat([main_df, df_result])
                logger.info('Dataframe created for: %s', extension)
            except Exception as ex:
                logger.error(ex, exc_info=True)

        self.save_result(main_df, post_date=post_date, local_file=True)

        return None


def back_fill_pipeline_date():
    scraper = GasNom(job_id=str(uuid.uuid4()))
    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        print(post_date)
        scraper.start_scraping(post_date)


def main():
    scraper = GasNom(job_id=str(uuid.uuid4()))
    # test for custom date
    scraper.start_scraping(date.fromisoformat('2022-07-15'))
    # for current date
    scraper.start_scraping()
    scraper.scraper_info()


if __name__ == '__main__':
    main()
