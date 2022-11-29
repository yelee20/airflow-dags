from typing import Optional, List
from airflow.utils.context import Context
from airflow.models import BaseOperator
from constants.data_category import DataCategory

from typing import Any


template_fields = (
    "bucket_name",
    "provider",
    # "batch_info",
    "data_category"
    "execution_date"
)
class HKPropertySourcingOperator(BaseOperator):

    def __init__(
            self,
            bucket_name: str,
            provider: str,
            data_category: str,
            execution_date: str,
            **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.provider = provider
        self.execution_date = execution_date
        self.data_category = data_category
        self.base_url = "https://www.hkp.com.hk/en/list/rent"

    def get_chrome_driver(self):
        from selenium import webdriver
        from selenium.webdriver.firefox.options import Options as FirefoxOptions

        options = FirefoxOptions()
        options.add_argument("--headless")
        options.add_argument("--width=700")
        options.add_argument("--height=700")
        driver = webdriver.Firefox(options=options)
        self.log.info("------- driver -------")

        return driver

    def scroll_to_end(self, driver, stand_height=0, retry_count=0):
        self.log.info("------- Scrolling -------")

        import time

        last_page_height = driver.execute_script(
            "return document.documentElement.scrollHeight"
        )
        sub_height = last_page_height

        while True:
            if retry_count >= 5:
                break

            current_height = driver.execute_script(
                "return document.documentElement.scrollHeight"
            )

            for i in range(10):
                driver.execute_script(
                    f"window.scrollTo(0, {stand_height + (sub_height / 10 * i)});"
                )
                time.sleep(0.5)
            time.sleep(4)

            new_page_height = driver.execute_script(
                "return document.documentElement.scrollHeight"
            )
            stand_height = last_page_height
            sub_height = new_page_height - last_page_height

            if new_page_height == current_height:
                self.scroll_to_end(driver, current_height, retry_count + 1)
                break
            last_page_height = new_page_height
            time.sleep(1)

            self.log.info(f"last_page_height: {last_page_height}")

        return driver.page_source

    def get_html_source(self, driver):
        import time
        from bs4 import BeautifulSoup
        self.log.info("------- Getting HTML source - STEP 1 -------")

        driver.implicitly_wait(3)
        driver.get(self.base_url)

        time.sleep(5)
        html_source = self.scroll_to_end(driver)
        self.log.info("------- Getting HTML source - SCROLLING -------")
        soup = BeautifulSoup(html_source, 'html.parser')
        return soup

    def get_sfa_gfa(self, space_info: List):
        if len(space_info) < 2:
            return None, None, None, None
        elif len(space_info) < 7:
            return space_info[1], space_info[3][1:], None, None
        else:
            return space_info[1], space_info[3][1:], space_info[5], space_info[7][1:]

    def get_property_info(self, soup):
        import re

        rooms = []
        rents = soup.find_all("div", class_="sc-u3x3v7-25 hBypJX")

        for rent in rents:
            titles = rent.find("div", class_="sc-hs8n9o-1 iaNTuL").get_text()
            title_list = titles.strip().split("\n")
            title = title_list[0].strip()

            if len(title_list) < 3:
                sub_title = None
            else:
                sub_title = title_list[2].strip()

            space = rent.find("div", class_="sc-16di5lh-5 cxijRp").get_text().replace("ft²", "")
            space_element = re.split(" |GFA|\\xa0", space)

            sfa, price_per_sfa, gfa, price_per_gfa = self.get_sfa_gfa(space_element)
            mon_price = rent.find("span", class_="sc-1fa9gj4-4 iPsFLJ").get_text()[1:]
            location = rent.find("span", class_="sc-u3x3v7-11 iPCCQr").get_text()
            features = rent.find_all("div", class_="sc-u3x3v7-18 gwyDpM")
            features_combined = ""

            for i in range(len(features) // 2):
                features_combined += features[i].get_text() + "\\ "

            age = rent.find("div", class_="sc-1prre98-0 jNGTki")

            if age:
                age = age.get_text()
            else:
                age = None

            url = rent.find('a', href=True)['href']
            room_idx = url.split("-")[-1]

            room_info = {"room_idx": room_idx, "title": title, "sub_title": sub_title,
                         "sfa": sfa,
                         "price_per_sfa": price_per_sfa, "gfa": gfa,
                         "price_per_gfa": price_per_gfa, "mon_price": mon_price, "age": age, "location": location,
                         "features_combined": features_combined, "url": url}

            if room_info not in rooms:
                rooms.append(room_info)

        result_objects = {"ResultObjects": rooms}
        self.log.info("------- Property Info -------")
        self.log.info(result_objects)
        self.log.info(f"# of properties: {len(rooms)}")
        return result_objects

    def upload_property_info_to_s3(self, json_data) -> None:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from utils.aws.s3 import upload_json_to_s3, get_sourcing_path
        from constants.constants import AWS_S3_CONN_ID

        self.log.info("------- Uploading property info to AWS S3 -------")

        sourcing_path = get_sourcing_path(provider=self.provider,
                                          data_category=self.data_category,
                                          execution_date=self.execution_date)
        s3_hook = S3Hook(AWS_S3_CONN_ID)

        upload_json_to_s3(s3_hook=s3_hook,
                          bucket_name=self.bucket_name,
                          data_key=f"{sourcing_path}.json",
                          json_data=json_data)

        self.log.info("------- PROPERTY INFO UPLOADED -------")

    def execute(self, context: Context) -> None:
        driver = self.get_chrome_driver()
        soup = self.get_html_source(driver)
        results = self.get_property_info(soup)

        self.upload_property_info_to_s3(results)
