import uuid
import logging
import re
import pandas as pd
from datetime import date, timedelta
from io import StringIO
from bs4 import BeautifulSoup

from scraper import PipelineScraper

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class BerkshireHathawayEnergy(PipelineScraper):
    source = 'dekaflow.bhegts'
    source_extensions = ['cpl', 'egts']
    api_url = 'https://dekaflow.bhegts.com/'
    get_url = 'https://dekaflow.bhegts.com/servlet/InfoPostServlet?region=null&company={}&method=headers&category=Capacity&subcategory=Operationally+Available'
    download_url = 'https://dekaflow.bhegts.com/servlet/InfoPostServlet?region=null&company={}&category=Capacity&subcategory=Operationally+Available&method=downloadText&filename=op_avail_infopost_seqnbr_{}_rev_0&extension=csv&seqNbr={}&revision=0'

    get_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'iframe',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }

    cycle_options = {
        1: 'Timely',
        2: 'Evening',
        3: 'Intraday 1',
        4: 'Intraday 2',
        5: 'Intraday 3'
    }

    def __init__(self, job_id):
        PipelineScraper.__init__(self, job_id, web_url=self.api_url, source=self.source)

    def get_download_url(self, source_ext, cycle: int = None, post_date: date = None):
        response = self.session.get(self.get_url.format(source_ext), headers=self.get_headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')

        pattern = r'.*?{}.*?{}'.format(post_date.strftime('%m/%d/%Y'), self.cycle_options[cycle])
        seq_no = ''

        # Find a match based on post date and cycle, then extract seq no from url
        for element in soup.find_all('a'):
            match_text = re.match(pattern, element.text)
            if match_text:
                seq_no_url = element['href']
                seq_no_search = re.search('(\d+)', seq_no_url)
                seq_no += seq_no_search.group(1)
                break

        final_download_url = self.download_url.format(source_ext, seq_no, seq_no)

        return final_download_url

    def start_scraping(self, cycle: int = None, post_date: date = None):
        post_date = post_date if post_date is not None else date.today()
        cycle = cycle if cycle is not None else 1

        main_df = pd.DataFrame()
        for extension in self.source_extensions:
            try:
                logger.info('Scraping %s/%s pipeline gas for post date: %s', self.source, extension, post_date)
                csv_url = self.get_download_url(extension, cycle, post_date)
                response = self.session.get(csv_url, headers=self.get_headers)
                response.raise_for_status()

                html_text = response.text
                csv_data = StringIO(html_text)

                df_result = pd.read_csv(csv_data)
                main_df = pd.concat([main_df, df_result])
                logger.info('Dataframe created. End of scraping: %s', extension)
            except Exception as ex:
                logger.error(ex, exc_info=True)

        # Remove 'Unnamed' column/s
        main_df = main_df.loc[:, ~main_df.columns.str.startswith('Unnamed')]
        self.save_result(main_df, post_date=post_date, local_file=True)

        return None


def back_fill_pipeline_date():
    scraper = BerkshireHathawayEnergy(job_id=str(uuid.uuid4()))
    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        print(post_date)
        scraper.start_scraping(post_date)


def main():
    scraper = BerkshireHathawayEnergy(job_id=str(uuid.uuid4()))
    custom_date = date.fromisoformat('2022-08-26')
    # Cycles: Timely = 1, Evening = 2, Intra Day 1 = 3, Intra Day 2 = 4, Intra Day 3 = 5
    set_cycle = 1
    scraper.start_scraping(post_date=custom_date, cycle=set_cycle)


if __name__ == '__main__':
    main()

