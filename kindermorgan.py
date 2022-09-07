import uuid
import logging
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date, timedelta

from scraper import PipelineScraper


logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class Kindermorgan(PipelineScraper):
    source = "pipeline2.kindermorgan"
    api_url = "https://pipeline2.kindermorgan.com/"
    post_data_url = "https://pipeline2.kindermorgan.com/Capacity/OpAvailPoint.aspx?code=RUBY"
    get_url = "https://pipeline2.kindermorgan.com/Capacity/OpAvailPoint.aspx?code=RUBY"

    post_page_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate,br',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Referer': 'https://pipeline2.kindermorgan.com/Capacity/OpAvailPoint.aspx?code=RUBY',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
        'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
        'sec-ch-ua-platform': 'Linux',
        'sec-ch-ua-mobile': '?0',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1'
    }

    def __init__(self, job_id):
        PipelineScraper.__init__(self, job_id, web_url=self.api_url, source=self.source)

    def get_payload(self, cycle: int, post_date=None):
        payload_post_date = post_date.strftime('%Y-%-m-%d')
        response = self.session.get(self.get_url)
        soup = BeautifulSoup(response.text, 'lxml')

        view_state = soup.find("input", {'id': '__VIEWSTATE'}).get('value')
        event_argument = soup.find("input", {'id': '__EVENTARGUMENT'}).get('value')
        event_target = soup.find("input", {'id': '__EVENTTARGET'}).get('value')
        view_state_generator = soup.find("input", {'id': '__VIEWSTATEGENERATOR'}).get('value')
        event_validation = soup.find("input", {'id': '__EVENTVALIDATION'}).get('value')

        payload = {
            'ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$DownloadDDL': 'EXCEL',
            'WebSplitter1_tmpl1_ContentPlaceHolder1_dtePickerBegin_clientState': '|0|01' + payload_post_date + '-0-0-0-0||[[[[]],[],[]],[{},[]],"01' + payload_post_date + '-0-0-0-0"]',
            '__EVENTTARGET': event_target,
            '__EVENTARGUMENT': event_argument,
            '__VIEWSTATE': view_state,
            '__VIEWSTATEGENERATOR': view_state_generator,
            '__EVENTVALIDATION': event_validation,
            '__ASYNCPOST': 'true',
            'WebSplitter1_tmpl1_ContentPlaceHolder1_ddlCycleDD_clientState': '|0|&tilda;2||[[[[null,null,null,null,null,null,null,-1,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,"TIMELY",null,null,null,null,null,null,null,null,null,null,null,null,null,0,0,null,null,1,null,null,null,null,null,null,null,null]],[],null],[{"0":[41,' + str(cycle) + '],"1":[7,' + str(cycle) + '],"2":[23,"EVENING"]},[{"0":[1,0,17],"1":["2",0,81],"2":[1,9,0],"3":["2",9,1],"5":["2",7,1],"6":[1,7,0]}]],null]',
            'ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.x': '50',
            'ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$HeaderBTN1$btnDownload.y': '11',
        }
        return payload

    def start_scraping(self, cycle=None, post_date=None):
        post_date = post_date if post_date is not None else date.today()
        cycle = cycle if cycle is not None else '1'
        locations = [{'ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$location': 'rbDelivery'},
                     {'ctl00$WebSplitter1$tmpl1$ContentPlaceHolder1$location': 'rbReceipt'}]

        main_df = pd.DataFrame()
        logger.info('Scraping %s pipeline gas for post date: %s', self.source, post_date)
        for loc in locations:
            try:
                payload = self.get_payload(cycle, post_date)
                payload.update(loc)
                response = self.session.post(self.post_data_url, data=payload, headers=self.post_page_headers)
                response.raise_for_status()
                excel_file = response.content

                df = pd.read_excel(excel_file, engine='openpyxl', header=None)
                report = self.format_columns(df)
                main_df = pd.concat([main_df, report])

            except Exception as ex:
                logger.error(ex, exc_info=True)

        self.save_result(main_df, post_date, local_file=True)

        return None

    def format_columns(self, df):
        # get column names and values from row 3 and below
        detail_columns = df.iloc[3].to_list()
        detail_data = df.iloc[4:-4]
        detail_data = detail_data.values.tolist()
        detail_df = pd.DataFrame(detail_data, columns=detail_columns)

        # get column names and values from rows 1-2 for the general info columns (i.e. TSP, TSP Name, etc.)
        info_columns = df.iloc[0].to_list()
        info_data = df.iloc[1].to_list()
        dict_list = [{info_columns[i]: info_data[i] for i in range(len(info_columns)) if pd.isnull(info_columns[i]) == False}]
        info_df = pd.DataFrame(dict_list)
        info_df = pd.concat([info_df]*len(detail_df.index), ignore_index=True)

        # combine all data
        final_df = pd.concat([info_df, detail_df], axis=1)

        return final_df


def back_fill_pipeline_date():
    scraper = Kindermorgan(job_id=str(uuid.uuid4()))
    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        scraper.start_scraping(post_date)


def main():
    # cycle options: TIMELY = 1,EVENING = 2, INTRADAY 1 = 3, INTRADAY 2 = 4, INTRADAY 3 = 5
    # default = TIMELY
    custom_cycle = 4
    custom_date = date.fromisoformat('2022-08-26')
    scraper = Kindermorgan(job_id=str(uuid.uuid4()))
    scraper.start_scraping(cycle=custom_cycle, post_date=custom_date)


if __name__ == '__main__':
    main()
