#!/usr/bin/python
"""
IDMC:
------------

Reads IDMC csvs and creates datasets.

"""

import logging
import re
from datetime import timedelta
from os.path import join

from slugify import slugify

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import DownloadError
from hdx.utilities.matching import multiple_replace
from hdx.utilities.path import script_dir_plus_file

logger = logging.getLogger(__name__)


class Pipeline:
    regex_popup = re.compile(r"(.*)<a href=\\?\"(.*)\\?\"target")

    def __init__(self, configuration, retriever, today, folder):
        self.configuration = configuration
        self.retriever = retriever
        self.today = today
        self.folder = folder
        self.events = {}
        self.countrymapping = {}
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
            income_level = countryinfo["#indicator+incomelevel"]
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
        for event in json:
            countryiso = event["iso3"]
            if countryiso not in self.idmc_territories:
                continue
            self.countrymapping[countryiso] = event["country"]

        for event in json:
            countryiso = event["iso3"]
            if countryiso not in self.countrymapping:
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
                "No countries with events in last 180 days which is highly improbable!"
            )
        first_list = next(iter(self.events.values()))
        self.headers = list(first_list[0].keys())

        territories_not_in_countries = self.idmc_territories.difference(
            countries_with_events
        )
        countries = [
            {"iso3": countryiso} for countryiso in sorted(territories_not_in_countries)
        ]
        countries.extend(
            [{"iso3": countryiso} for countryiso in sorted(countries_with_events)]
        )
        return countries

    def generate_dataset_and_showcase(self, countryiso):
        prefix = "idmc event data for "
        name = f"{prefix}{countryiso}"
        countryname = Country.get_country_name_from_iso3(countryiso)
        if not self.headers:
            logger.error(
                f"Headers not populated. Cannot update {countryname} that has no events!"
            )
            return None, None
        title = f"{countryname} - Internal Displacements Updates (IDU) (event data)"
        dataset = Dataset({"name": slugify(name).lower(), "title": title})
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("647d9d8c-4cac-4c33-b639-649aad1c2893")
        dataset.set_expected_update_frequency("Every day")
        dataset.set_time_period(self.today - timedelta(days=180), self.today)
        dataset.set_subnational(False)
        try:
            dataset.add_country_location(countryiso)
        except HDXError as e:
            logger.exception(f"{countryname} has a problem! {e}")
            return None, None
        description = self.configuration["description"]
        filename = f"event_data_{countryiso}.csv"
        resourcedata = {
            "name": name,
            "description": f"{prefix}{countryname}",
        }
        rows = self.events.get(countryiso, [])
        tags = {"displacement", "internally displaced persons-idp"}
        for row in rows:
            subtype = row["subtype"]
            if subtype is None:
                subtype = row["displacement_type"]
            tags.update(subtype.split("/"))
        tags = sorted(tags)
        dataset.add_tags(tags)
        if rows:
            dataset["notes"] = (
                f"Conflict and disaster population movement (flows) data for {countryname}. \n\n{description}"
            )
        else:
            dataset["notes"] = (
                f"**Resource has no data rows!** No conflict and disaster population movement (flows) data recorded for {countryname} in the last 180 days.\n\n{description}"
            )
            resourcedata["description"] += "  \n**Resource has no data rows!**"

        dataset.generate_resource(
            self.folder,
            filename,
            rows,
            resourcedata,
            headers=self.headers,
            no_empty=False,
        )
        internal_countryname = self.countrymapping.get(countryiso)
        if not internal_countryname:
            return dataset, None

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
