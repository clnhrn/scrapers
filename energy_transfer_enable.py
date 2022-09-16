import uuid
from datetime import date, timedelta
import logging

import pandas as pd
from bs4 import BeautifulSoup

from scraper import PipelineScraper


logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class EnergyTransferEnable(PipelineScraper):
    parent_source = 'energytransfer'
    tsp_list = [
        {
            'source': 'pipelines.energytransfer',
            'home_url': 'https://pipelines.energytransfer.com/ipost/EGT/main/index',
            'api_url': 'https://pipelines.energytransfer.com',
            'get_url': 'https://pipelines.energytransfer.com/ipost/EGT/capacity/enbl-operationally-available?max=ALL',
            'tsp_id': '872670161',
            'tsp_name': 'ENABLE GAS TRANSMISSION, LLC'
        },
        {
            'source': 'pipelines.energytransfer',
            'home_url': 'https://pipelines.energytransfer.com/ipost/MRT/main/index',
            'api_url': 'https://pipelines.energytransfer.com',
            'get_url': 'https://pipelines.energytransfer.com/ipost/MRT/capacity/enbl-operationally-available?max=ALL',
            'tsp_id': '006968077',
            'tsp_name': 'ENABLE MISSISSIPPI RIVER TRANSMISSION, LLC'
        }
    ]

    get_page_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,fil;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Host': 'pipelines.energytransfer.com',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
          }

    cycle_options = {
        0: 'TIMELY',
        1: 'EVENING',
        2: 'INTRADAY 1',
        3: 'INTRADAY 2',
        4: 'INTRADAY 3',
        5: 'FINAL'
    }

    def __init__(self, job_id):
        PipelineScraper.__init__(self, job_id, web_url=self.tsp_list[0]['api_url'], source=self.parent_source)

    def get_report_list(self, url):
        response = self.session.get(url, headers=self.get_page_headers, timeout=120)
        return response.text

    def get_addtl_info(self, df_data):
        info_dict = {}
        for line in df_data.split('\n'):
            if 'Posting Date/Posting Time' in line:
                info = line.split(':', 1)
                info_dict.update({info[0].strip(): info[1].strip()})
            if 'Effective Gas Day/Effective Time' in line:
                info = line.split(':', 1)
                info_dict.update({info[0].strip(): info[1].strip()})
            if 'Meas Basis Desc' in line:
                info = line.split(':', 1)
                info_dict.update({info[0].strip(): info[1].strip()})
            if 'Cycle' in line:
                info = line.split(':', 1)
                info_dict.update({info[0].strip(): info[1].strip()})
                break

        return info_dict

    def add_columns(self, tsp, report, details):
        report.insert(0, 'TSP', tsp['tsp_id'], True)
        report.insert(1, 'TSP Name', tsp['tsp_name'], True)
        report.insert(2, 'Post Date/Time', details['Posting Date/Posting Time'], True)
        report.insert(3, 'Effective Gas Day/Time', details['Effective Gas Day/Effective Time'], True)
        report.insert(4, 'Cycle', details['Cycle'], True)
        report.insert(5, 'Meas Basis Desc', details['Meas Basis Desc'], True)
        report.insert(18, 'Qty Reason', ' ', True)

        return report

    def start_scraping(self, post_date: date = None, cycle: int = 0):
        post_date = post_date if post_date is not None else date.today().strftime('%Y-%m-%d')
        matches = [post_date.strftime('%Y-%m-%d'), self.cycle_options[cycle]]

        main_df = pd.DataFrame()
        for tsp in self.tsp_list:
            try:
                logger.info('Scraping %s pipeline gas for post date: %s', tsp['source'], post_date)
                reports = self.get_report_list(tsp['get_url'])
                soup = BeautifulSoup(reports, 'lxml')
                user_report = soup.find_all('td', class_='reportTitle text-center')
                for rep in user_report:
                    report_cell = rep.text.strip()
                    # checks which report_cell contains all elements in the matches list.
                    if all(words in report_cell for words in matches):
                        view_report_elem = rep.parent.findChild('td', class_='id').findChild(href=True)
                        view_report_link = view_report_elem['href']
                        csv_url = tsp['api_url'] + view_report_link

                        page_response = self.session.get(csv_url, headers=self.get_page_headers)
                        page_response.raise_for_status()

                        html_text = page_response.text
                        soup = BeautifulSoup(html_text, 'lxml')
                        pre = soup.select_one('pre').text
                        column_sep_index = [0, 8, 34, 39, 59, 65, 73, 87, 99, 111, 125, 145, 153]
                        columns = ['Loc', 'Location Name', 'Loc Zone', 'Loc Purpose', 'Loc/QTI', 'All Qty Avail',
                                   'Design Capacity', 'Operating Capacity', 'Total Scheduled Quantity',
                                   'Operationally Available Capacity', 'Flow Ind Desc', 'IT Desc']
                        rows = []
                        # splits the preformatted table every after new line (\n).
                        for line in pre.split('\n')[15:-1]:
                            if 'COMMENTS AND NOTES' not in line:
                                # divides each line into parts where each belong to a corresponding column in the report.
                                parts = [line[column_sep_index[i]:column_sep_index[i + 1]] for i in range(len(column_sep_index) - 1)]
                                row = [x.strip() for x in parts[0:12]]
                                rows.append(row)
                            else:
                                break

                        rows.pop()
                        df = pd.DataFrame(rows, columns=columns)
                        report_details = self.get_addtl_info(pre)
                        final_report = self.add_columns(tsp, df, report_details)
                        main_df = pd.concat([main_df, final_report])
                        logger.info('DF created for %s', tsp['source'])
                    else:
                        continue

            except Exception as ex:
                logger.error(ex, exc_info=True)

        self.save_result(main_df, post_date=post_date, local_file=True)
        logger.info('File saved. end of scraping: %s', self.parent_source)

        return None


def back_fill_pipeline_date():
    scraper = EnergyTransferEnable(job_id=str(uuid.uuid4()))
    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        print(post_date)
        scraper.start_scraping()


def main():
    # set your own date to scrape. default is current date.
    custom_date = date.fromisoformat('2022-09-11')
    # set desired cycle:
    # 0 = Timely (Default),
    # 1 = Evening,
    # 2 = ID1,
    # 3 = ID2,
    # 4 = ID3,
    # 5 = Final - csv file might be empty if not yet available
    custom_cycle = 2
    scraper = EnergyTransferEnable(job_id=str(uuid.uuid4()))
    scraper.start_scraping(cycle=custom_cycle, post_date=custom_date)
    scraper.scraper_info()


if __name__ == '__main__':
    main()
