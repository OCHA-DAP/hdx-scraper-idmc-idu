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

    def get_countriesdata(self, state):
        url = self.configuration["url"]
        json = self.retriever.download_json(url, "idmc_idu.json")
        updated_countries = set()
        unknown_countryisos = set()
        high_income_countries = set()
        for event in json:
            countryiso = event["iso3"]
            countryinfo = Country.get_country_info_from_iso3(countryiso)
            if not countryinfo:
                unknown_countryisos.add(countryiso)
                continue
            countryname = event["country"]
            income_level = countryinfo["#indicator+incomelevel"]
            if income_level.lower() == "high":
                high_income_countries.add(countryname)
                continue
            self.countrymapping[countryiso] = countryname
            date = parse_date(event["created_at"][:10])
            if date > state.get(countryiso, state["DEFAULT"]):
                state[countryiso] = date
                updated_countries.add(countryiso)
        unknown_countryisos = ",".join(unknown_countryisos)
        logger.warning(f"Ignoring unknown country isos: {unknown_countryisos}")
        logger.info(f"Ignoring high income countries {high_income_countries}!")

        for event in json:
            countryiso = event["iso3"]
            if countryiso in updated_countries:
                popup = event["standard_popup_text"].replace("\n", " ")
                match = self.regex_popup.match(popup)
                text, link = match.groups()
                text = multiple_replace(
                    text, {"<br>": "", "<b>": "", "</b>": ".", "\t": " "}
                )
                text = re.sub(" +", " ", text)
                event["description"] = text.replace(" .", ".").strip()
                event["link"] = link
                del event["standard_popup_text"]
                del event["standard_info_text"]
                event_type = event["type"]
                if event_type:
                    event["combined_type"] = event_type
                else:
                    event["combined_type"] = event["displacement_type"]
                dict_of_lists_add(self.events, countryiso, event)
        return [{"iso3": countryiso} for countryiso in sorted(self.events)]

    def generate_dataset_and_showcase(self, countryiso):
        name = f"idmc event data for {countryiso}"
        countryname = Country.get_country_name_from_iso3(countryiso)
        title = f"{countryname} - IDMC Event data (Internal Displacement Updates)"
        dataset = Dataset({"name": slugify(name).lower(), "title": title})
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("647d9d8c-4cac-4c33-b639-649aad1c2893")
        dataset.set_expected_update_frequency("Every year")
        dataset.set_subnational(False)
        try:
            dataset.add_country_location(countryiso)
        except HDXError as e:
            logger.exception(f"{countryname} has a problem! {e}")
            return None, None
        description = self.configuration["description"]
        dataset[
            "notes"
        ] = f"Conflict and disaster population movement (flows) data for {countryname}. {description}"
        filename = f"event_data_{countryiso}.csv"
        resourcedata = {
            "name": name,
            "description": f"{name} for {countryname}",
        }
        tags = set()

        def process_dates(row):
            subtype = row["subtype"]
            if subtype is None:
                subtype = row["displacement_type"]
            tags.update(subtype.split("/"))
            event_startdate = parse_date(row["event_start_date"])
            displacement_startdate = parse_date(row["displacement_start_date"])
            startdate = min(event_startdate, displacement_startdate)
            event_enddate = parse_date(row["event_end_date"])
            displacement_enddate = parse_date(row["displacement_end_date"])
            enddate = max(event_enddate, displacement_enddate)
            return {"startdate": startdate, "enddate": enddate}

        rows = self.events[countryiso]
        success, _ = dataset.generate_resource_from_iterator(
            list(rows[0].keys()),
            rows,
            self.configuration["hxltags"],
            self.folder,
            filename,
            resourcedata,
            date_function=process_dates,
        )
        tags.add("hxl")
        tags.add("displacement")
        tags.add("internally displaced persons-idp")
        tags = sorted(tags)
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
