import uuid
import zipfile
import logging

import pandas as pd
from datetime import date, datetime, timedelta
from requests import HTTPError
from io import BytesIO
from scraper import PipelineScraper

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

"""
NOTE: This Scraper downloads zip which contains all the data. There is nothing to scrape from web as it has no web interface
to scrape from table.
"""


class BigSandy(PipelineScraper):
    source = "rtba.enbridge.com"
    api_url = 'https://rtba.enbridge.com'
    post_url = 'https://rtba.enbridge.com/InformationalPosting/Download.aspx/StartFile'
    start_file_url = 'https://rtba.enbridge.com/InformationalPosting/Download.aspx/StartFile'
    add_to_file_url = 'https://rtba.enbridge.com/InformationalPosting/Download.aspx/AddToFile'
    zip_file_url = 'https://rtba.enbridge.com/InformationalPosting/Download.aspx/ZipFile'
    file_handler_url = 'https://rtba.enbridge.com/InformationalPosting/HttpHandlers/FileHandler.ashx'

    init_request_params = {
        "businessUnitAbbreviation": "BSP",
        "postingType": "OA",
        "postingSubType": "MLC",
        "fileType": "csv",
        "cycle": "All",
    }

    file_handle_params = {
        "postingAbbreviation": "OA",
        "postingSubType": "MLC",
        "startGasDate": "08/10/2022",
        "endGasDate": "08/10/2022"
    }

    get_page_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'rtba.enbridge.com',
        'Origin': 'https://rtba.enbridge.com',
        'Referer': 'https://rtba.enbridge.com/InformationalPosting/Download.aspx?bu=BSP&type=OA',
        'sec-ch-ua': '"Chromium";v="104", " Not A;Brand";v="99", "Google Chrome";v="104"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'TE': 'trailers',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'
    }

    def __init__(self, job_id):
        PipelineScraper.__init__(self, job_id, web_url=self.api_url, source=self.source)

    def set_request_params_date(self, post_date: date):
        self.init_request_params['currentDate'] = post_date.strftime("%m/%d/%Y")
        self.init_request_params['startGasDate'] = post_date.strftime("%m/%d/%Y")
        self.init_request_params['endGasDate'] = post_date.strftime("%m/%d/%Y")

    def set_file_handle_params_date(self, post_date: date):
        self.file_handle_params['startGasDate'] = post_date.strftime("%m/%d/%Y")
        self.file_handle_params['endGasDate'] = post_date.strftime("%m/%d/%Y")

    def start_scraping(self, post_date: date = None):
        post_date = post_date if post_date is not None else date.today()
        try:
            logger.info('Scraping %s pipeline gas for post date: %s', self.source, post_date)
            self.set_request_params_date(post_date=post_date)

            # Start the request for preparation of zip file
            response = self.session.post(self.start_file_url, json=self.init_request_params)
            response.raise_for_status()
            file_name = response.json()['d']

            self.init_request_params.update({'fileName': file_name})

            # add files and prepare zip
            response = self.session.post(self.add_to_file_url, json=self.init_request_params)
            # print(response.json())

            # prepare server for zip request for file
            response = self.session.post(self.zip_file_url, json={'fileName': file_name})
            # print(response.content)

            local_filename = f"./DATA/scraper_output/{file_name}.zip"
            self.file_handle_params.update({'fileName': file_name})

            # set param for filehandler.ashx call
            self.set_file_handle_params_date(post_date=post_date)

            # this call gets the data as zip file from server
            with self.session.post(self.file_handler_url, data=self.file_handle_params,
                                   headers=self.get_page_headers, stream=True) as r:
                r.raise_for_status()
                zip_ref = zipfile.ZipFile(BytesIO(r.content))

                df_list = []

                # read all csv inside the zip file
                for name in zip_ref.namelist():
                    with zip_ref.open(name) as file_contents:
                        df = pd.read_csv(file_contents)
                        df_list.append(df)

                final_report = pd.concat(df_list)
                self.save_result(final_report, post_date=post_date, local_file=True)
        except HTTPError as ex:
            logger.error(ex, exc_info=True)
        return None


def back_fill_pipeline_date():
    scraper = BigSandy(job_id=str(uuid.uuid4()))
    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        print(post_date)
        scraper.start_scraping(post_date)


def main():
    query_date = datetime.fromisoformat("2022-08-11")

    # there are no cycle to choose in Viking Gas Transmission
    scraper = BigSandy(job_id=str(uuid.uuid4()))
    scraper.start_scraping()
    scraper.scraper_info()


if __name__ == '__main__':
    main()
