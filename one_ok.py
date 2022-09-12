import uuid
import json
import logging
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta

from requests import HTTPError

from scraper import PipelineScraper

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class OneOK(PipelineScraper):
    source = "oneok.com"
    source_extensions = ['vgt', 'mgt', 'gpl']
    api_url = 'https://www.oneok.com'
    post_url = 'https://www.oneok.com/{}/informational-postings/capacity/operationally-available'
    download_csv_url = 'https://www.oneok.com/{}/informational-postings/capacity/operationally-available'

    html_header_css = ".rgHeaderDiv .rgMasterTable"
    html_table_css = ".rgDataDiv > .rgMasterTable"
    params = {}

    def __init__(self, job_id):
        PipelineScraper.__init__(self, job_id, web_url=self.api_url, source=self.source)

    def get_headers(self, soup: BeautifulSoup):
        theads = soup.find_all("th", {"class": "rgHeader"})
        headers = [th.findNext("a").text for th in theads]
        return headers

    def get_page_request(self, ext, post_date: date = None):
        response = self.session.get(self.post_url.format(ext))
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')

        view_state = soup.find("input", {'type': 'hidden', 'id': '__VIEWSTATE'}).get("value")
        event_validation = soup.find("input", {"type": "hidden", "id": "__EVENTVALIDATION"}).get("value")

        self.params['__VIEWSTATE'] = view_state
        self.params['__EVENTVALIDATION'] = event_validation

    def set_params(self, post_date: date):
        str_post_date = post_date.strftime("%Y-%m-%d")
        self.params['content_1$dcDateControls$rdpStartDate'] = post_date.strftime("%Y-%m-%d")

        self.params['content_1$dcDateControls$rdpStartDate$dateInput'] = post_date.strftime("%m/%d/%Y")

        rdp_startdate_dateinput_clientstate = {
            "enabled": True,
            "emptyMessage": "",
            "validationText": f"{str_post_date}-00-00-00",
            "valueAsString": f"{str_post_date}-00-00-00",
            "minDateStr": f"{str_post_date}-00-00-00",
            "maxDateStr": f"{str_post_date}-00-00-00",
            "lastSetTextBoxValue": f"{post_date.strftime('%m/%d/%Y')}"
        }

        self.params['content_1_dcDateControls_rdpStartDate_dateInput_ClientState'] = json.dumps(
            rdp_startdate_dateinput_clientstate)

        self.params['content_1_dcDateControls_rdpEndDate_ClientState'] = json.dumps(
            {"minDateStr": f"{str_post_date}-00-00-00",
             "maxDateStr": f"{str_post_date}-00-00-00"})

        self.params['content_1$dcDateControls$btnRefresh'] = 'Refresh'

    def start_scraping(self, post_date: date = None):
        post_date = post_date if post_date is not None else date.today()

        main_df = pd.DataFrame()
        for extension in self.source_extensions:
            try:
                logger.info('Scraping %s pipeline gas for post date: %s', self.source, post_date)
                self.set_params(post_date=post_date)
                self.get_page_request(extension, post_date=post_date)

                response = self.session.post(self.download_csv_url.format(extension), data=self.params)
                response.raise_for_status()

                soup = BeautifulSoup(response.content, 'lxml')

                headers = self.get_headers(soup)

                df_result = pd.read_html(str(soup.select_one(self.html_table_css)), na_values='')[0]
                df_result.columns = headers
                final_report = self.add_columns(soup=soup, df_result=df_result)
                main_df = pd.concat([main_df, final_report])

            except HTTPError as ex:
                logger.error(ex, exc_info=True)

        self.save_result(main_df, post_date=post_date, local_file=True)

        return None

    def add_columns(self, soup, df_result):
        tsp = soup.select_one("#content_1_CompanyDUNS").text
        tsp_name = soup.select_one("#content_1_CompanyName").text
        df_result.insert(0, 'TSP', tsp, True)
        df_result.insert(1, 'TSP Name', tsp_name, True)
        return df_result


def back_fill_pipeline_date():
    scraper = OneOK(job_id=str(uuid.uuid4()))
    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        print(post_date)
        scraper.start_scraping(post_date)


def main():
    query_date = datetime.fromisoformat("2022-08-11")

    # there are no cycles to choose from in OneOK websites
    scraper = OneOK(job_id=str(uuid.uuid4()))
    scraper.start_scraping(post_date=query_date)
    scraper.scraper_info()


if __name__ == '__main__':
    main()
