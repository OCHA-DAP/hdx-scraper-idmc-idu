#!/usr/bin/python
"""
IDMC:
------------

Reads IDMC HXLated csvs and creates datasets.

"""
import logging
import re
from operator import itemgetter

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import DownloadError
from hdx.utilities.text import multiple_replace
from slugify import slugify

logger = logging.getLogger(__name__)


class IDMC:
    regex_popup = re.compile(r"(.*)<a href=\\?\"(.*)\\?\"target")

    def __init__(self, configuration, retriever, folder):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.countries = set()
        self.events = {}
        self.countrymapping = {}

    @staticmethod
    def get_dataset(title, tags, name):
        logger.info(f"Creating dataset: {title}")
        dataset = Dataset({"name": slugify(name).lower(), "title": title})
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("647d9d8c-4cac-4c33-b639-649aad1c2893")
        dataset.set_expected_update_frequency("Every year")
        dataset.set_subnational(False)
        dataset.add_tags(tags)
        return dataset

    def get_country_data(self, state):
        url = self.configuration["url"]
        json = self.retriever.download_json(url, "idmc_idu.json")
        for event in json:
            countryiso = event["iso3"]
            self.countrymapping[countryiso] = event["country"]
            date = parse_date(event["created_at"])
            if date > state.get(countryiso, state["DEFAULT"]):
                state[countryiso] = date
                self.countries.add(countryiso)
                popup = event["standard_popup_text"].replace("\n", " ")
                match = self.regex_popup.match(popup)
                text, link = match.groups()
                text = multiple_replace(text, {"<br>": "", "<b>": "", "</b>": ".", "\t": " "})
                text = re.sub(" +", " ", text)
                event["description"] = text.replace(" .", ".").strip()
                event["link"] = link
                del event["standard_popup_text"]
                del event["standard_info_text"]
                dict_of_lists_add(self.events, countryiso, event)
        return [{"iso3": countryiso} for countryiso in sorted(self.countries)]

    def generate_dataset_and_showcase(self,
        countryiso
    ):
        name = f"idmc event data for {countryiso}"
        countryname = Country.get_country_name_from_iso3(countryiso)
        title = f"{countryname} - IDMC Event data"
        dataset = Dataset({"name": slugify(name).lower(), "title": title})
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("647d9d8c-4cac-4c33-b639-649aad1c2893")
        dataset.set_expected_update_frequency("Every year")
        dataset.set_subnational(False)
        try:
            dataset.add_country_location(countryiso)
        except HDXError as e:
            logger.exception(f"{countryname} has a problem! {e}")
            return None, None, None

        dataset["notes"] = "\n\n".join(self.configuration["notes"].values())
        filename = f"event_data_{countryiso}.csv"
        resourcedata = {
            "name": name,
            "description": f"{name} for {countryname}",
        }
        tags = set()

        def process_dates(row):
            tags.add(row["subtype"])
            event_startdate = parse_date(row["event_start_date"])
            displacement_startdate = parse_date(row["displacement_start_date"])
            startdate = min(event_startdate, displacement_startdate)
            event_enddate = parse_date(row["event_end_date"])
            displacement_enddate = parse_date(row["displacement_end_date"])
            enddate = max(event_enddate, displacement_enddate)
            return {"startdate": startdate, "enddate": enddate}

        rows = self.events[countryiso]
        success, _ = dataset.generate_resource_from_iterator(
            rows[0].keys(),
            rows,
            self.configuration["hxltags"],
            self.folder,
            filename,
            resourcedata,
            date_function=process_dates,
        )
        tags = list(tags)
        dataset.add_tags(tags)
        internal_countryname = self.countrymapping[countryiso]
        url = f"http://www.internal-displacement.org/countries/{internal_countryname.replace(' ', '-')}/"
        try:
            self.retriever.downloader.setup(url)
        except DownloadError:
            return dataset, None
        showcase = Showcase(
            {
                "name": f"{dataset['name']}-showcase",
                "title": f"IDMC {countryname} Summary Page",
                "notes": f"Click the image to go to the IDMC summary page for the {countryname} dataset",
                "url": url,
                "image_url": "https://www.internal-displacement.org/sites/default/files/logo_0.png",
            }
        )
        showcase.add_tags(tags)
        return dataset, showcase
