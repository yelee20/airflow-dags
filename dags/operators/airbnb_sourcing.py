from functools import cached_property
from typing import Optional
from airflow.utils.context import Context

from airflow.models import BaseOperator


class AirbnbSourcingOperator(BaseOperator):
    # template_fields = (
    #     "bucket_name",
    #     "mall_id",
    #     "provider",
    #     "data_category",
    #     "batch_info",
    #     "execution_date",
    #     "data_id",
    #     "ad_account_dict",
    #     "api_key",
    #     "secret_key",
    #     "customer_id",
    # )
    from typing import Any

    def __init__(
            self,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.http_conn_id = "airbnb"
        self.check_in_date = "2022-10-21"
        self.check_out_date = "2022-10-22"

    @staticmethod
    def get_url(page: Optional[int], check_in_date: str, check_out_date: str) -> str:
        if page != 1:
            return f"""https://www.airbnb.com/s/Hong-Kong/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&flexible_trip_lengths%5B%5D=one_week&price_filter_input_type=0&query=Hong%20Kong&place_id=ChIJByjqov3-AzQR2pT0dDW0bUg&date_picker_type=calendar&source=structured_search_input_header&search_type=filter_change&checkin={check_in_date}&checkout={check_out_date}&adults=2&price_filter_num_nights=1&ne_lat=22.912935588666485&ne_lng=114.51626632678347&sw_lat=21.986421057169483&sw_lng=114.02352307307558&zoom=10&search_by_map=true&federated_search_session_id=a35992c1-e192-4040-9bdc-98c98c7e916b&pagination_search=true&items_offset={(page - 1) * 20}&section_offset=1&display_currency=USD"""
        else:
            return f"""https://www.airbnb.com/s/Hong-Kong/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&flexible_trip_lengths%5B%5D=one_week&price_filter_input_type=0&query=Hong%20Kong&place_id=ChIJByjqov3-AzQR2pT0dDW0bUg&date_picker_type=calendar&source=structured_search_input_header&search_type=filter_change&checkin={check_in_date}&checkout={check_out_date}&adults=2&price_filter_num_nights=1&ne_lat=22.912935588666485&ne_lng=114.51626632678347&sw_lat=21.986421057169483&sw_lng=114.02352307307558&zoom=10&search_by_map=true&federated_search_session_id=a35992c1-e192-4040-9bdc-98c98c7e916b&pagination_search=true&display_currency=USD"""

    @staticmethod
    def max_page(url: str) -> int:
        from bs4 import BeautifulSoup as bs
        from urllib.request import urlopen
        page = urlopen(url)
        soup = bs(page, "html.parser")
        return int(soup.find_all("a", {"class": "_833p2h"})[-1].get_text())

    def get_room_list(self):
        from bs4 import BeautifulSoup as bs
        from urllib.request import urlopen

        room_infos = []
        url = self.get_url(page=1, check_in_date=self.check_in_date, check_out_date=self.check_out_date)

        for page in range(1, self.max_page(url) + 1):
            self.log.info(f"page: {page}")

            url = self.get_url(page=page, check_in_date=self.check_in_date, check_out_date=self.check_out_date)
            self.log.info(f"url: {url}")

            page = urlopen(url)
            soup = bs(page, "html.parser")
            titles = soup.find_all("div", itemprop="itemListElement")

            # TODO: 코드 개선
            for title in titles:
                result_url = title.find("a")["href"]
                room_idx = result_url[result_url.index('s/') + 2:result_url.index('?')]

                room_title = title.find("div", {"id": f"title_{room_idx}"}).get_text()

                try:
                    room_price0 = title.find("span", {"class": "_1ks8cgb"}).get_text()
                except:
                    room_price0 = None
                room_price1 = title.find("span", {"class": "_tyxjp1"}).get_text()
                room_price2 = title.find("div", {"class": "_tt122m"}).get_text().replace(" total", "")

                try:
                    room_type = title.find("span", {"class": "dir dir-ltr"}).get_text()
                except:
                    room_type = None

                ratings = title.find("span", {"class": "r1dxllyb dir dir-ltr"}).get_text()

                try:
                    host_type = title.find("div", {"class": "t1mwk1n0 dir dir-ltr"}).get_text()
                except:
                    host_type = None

                room_info = {"room_idx": room_idx, "room_title": room_title, "room_type": room_type,
                             "host_type": host_type,
                             "original_room_price": room_price0, "final_room_price": room_price1,
                             "total_room_price": room_price2, "ratings": ratings}

                if room_info not in room_infos:
                    room_infos.append(room_info)
            print(len(room_infos))
            result_objects = {"ResultObjects": room_infos}

            return result_objects

    def execute(self, context: Context) -> Any:
        room_list = self.get_room_list()
        print(room_list)