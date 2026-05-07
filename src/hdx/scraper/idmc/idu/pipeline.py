#!/usr/bin/python
"""
IDMC:
------------

Reads IDMC csvs and creates datasets.

"""

import logging
import re
from os.path import join

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import DownloadError
from hdx.utilities.matching import multiple_replace
from hdx.utilities.path import script_dir_plus_file
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    regex_popup = re.compile(r"(.*)<a href=\\?\"(.*)\\?\"target")

    def __init__(self, configuration, retriever, today, folder):
        self.configuration = configuration
        self.retriever = retriever
        self.today = today
        self.folder = folder
        self.events = {}
        self.countrynamemapping = {}
        self.countrystartdate = {}
        self.countryenddate = {}
        self.idmc_territories = set()
        self.headers = None

    def get_idmc_territories(self):
        headers, iterator = self.retriever.downloader.get_tabular_rows(
            script_dir_plus_file(join("config", "IDMC_territories.csv"), Pipeline),
            dict_form=True,
        )
        unknown_countryisos = set()
        high_income_countries = set()
        for row in iterator:
            countryiso = row["iso3"]
            countryinfo = Country.get_country_info_from_iso3(countryiso)
            if not countryinfo:
                unknown_countryisos.add(countryiso)
                continue
            countryname = row["idmc_short_name"]
            income_level = countryinfo["World Bank Income Level"] or ""
            if income_level.lower() == "high":
                high_income_countries.add(countryname)
                continue
            self.idmc_territories.add(row["iso3"])
        unknown_countryisos = ",".join(unknown_countryisos)
        logger.warning(f"Ignoring unknown country isos: {unknown_countryisos}")
        logger.info(f"Ignoring high income countries {high_income_countries}!")

    def get_countriesdata(self):
        url = self.configuration["url"]
        json = self.retriever.download_json(url, "idmc_idu.json")
        min_date = f"{self.today.year - 1}-01-01"
        for event in json:
            countryiso = event["iso3"]
            if countryiso not in self.idmc_territories:
                continue
            end_date = event["displacement_end_date"]
            if end_date < min_date:
                continue
            start_date = event["displacement_start_date"]
            min_start_date = self.countrystartdate.get(countryiso)
            if min_start_date:
                if start_date < min_start_date:
                    self.countrystartdate[countryiso] = start_date
            else:
                self.countrystartdate[countryiso] = start_date
            max_end_date = self.countryenddate.get(countryiso)
            if max_end_date:
                if end_date > max_end_date:
                    self.countryenddate[countryiso] = end_date
            else:
                self.countryenddate[countryiso] = end_date
            self.countrynamemapping[countryiso] = event["country"]

        for event in json:
            countryiso = event["iso3"]
            if countryiso not in self.countrynamemapping:
                continue
            end_date = event["displacement_end_date"]
            if end_date < min_date:
                continue
            popup = event["standard_popup_text"].replace("\n", " ")
            match = self.regex_popup.match(popup)
            if match:
                text, link = match.groups()
            else:
                text = popup
                link = None
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
        countries_with_events = set(self.events)
        if len(countries_with_events) == 0:
            raise ValueError(
                f"No countries with events since {min_date} which is highly improbable!"
            )
        first_list = next(iter(self.events.values()))
        self.headers = list(first_list[0].keys())

        return [{"iso3": countryiso} for countryiso in sorted(self.idmc_territories)]

    def generate_dataset_and_showcase(self, countryiso):
        prefix = "idmc event data for "
        name = f"{prefix}{countryiso}"
        countryname = Country.get_country_name_from_iso3(countryiso)
        title = f"{countryname} - Internal Displacements Updates (IDU) (event data)"
        dataset_name = slugify(name).lower()
        dataset = Dataset({"name": dataset_name, "title": title})
        try:
            dataset.add_country_location(countryiso)
        except HDXError as e:
            logger.exception(f"{countryname} has a problem! {e}")
            return None, None, False
        if countryiso not in self.countrynamemapping:
            return dataset, None, False
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("647d9d8c-4cac-4c33-b639-649aad1c2893")
        dataset.set_expected_update_frequency("As needed")
        dataset.set_time_period(
            self.countrystartdate[countryiso], self.countryenddate[countryiso]
        )
        dataset.set_subnational(False)
        description = self.configuration["description"].format(self.today.year - 1)
        filename = f"event_data_{countryiso}.csv"
        resourcedata = {
            "name": name,
            "description": f"{prefix}{countryname}",
        }
        rows = self.events[countryiso]
        tags = {"displacement", "internally displaced persons-idp"}
        for row in rows:
            subtype = row["subtype"]
            if subtype is None:
                subtype = row["displacement_type"]
            tags.update(subtype.split("/"))
        tags = sorted(tags)
        dataset.add_tags(tags)
        dataset["notes"] = (
            f"Conflict and disaster population movement (flows) data for {countryname}. \n\n{description}"
        )

        dataset.generate_resource(
            self.folder,
            filename,
            rows,
            resourcedata,
            headers=self.headers,
            no_empty=False,
        )
        internal_countryname = self.countrynamemapping.get(countryiso)
        if not internal_countryname:
            return dataset, None, True

        url = f"http://www.internal-displacement.org/countries/{internal_countryname.replace(' ', '-')}/"
        try:
            self.retriever.downloader.setup(url)
        except DownloadError:
            return dataset, None, True
        showcase = Showcase(
            {
                "name": f"{dataset_name}-showcase",
                "title": f"IDMC {countryname} Summary Page",
                "notes": f"Click the image to go to the IDMC summary page for the {countryname} dataset",
                "url": url,
                "image_url": "https://www.internal-displacement.org/sites/default/files/logo_0.png",
            }
        )
        showcase.add_tags(tags)
        return dataset, showcase, True
