#!/usr/bin/python
"""
Unit tests for IDMC

"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent
from idmc import IDMC


class TestIDMC:
    ind_dataset = {
        "data_update_frequency": "1",
        "dataset_date": "[2023-05-18T00:00:00 TO 2023-11-14T23:59:59]",
        "groups": [{"name": "ind"}],
        "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
        "name": "idmc-event-data-for-ind",
        "notes": "Conflict and disaster population movement (flows) data for India. "
        "The data is the most recent available and covers a 180 day time "
        "period.\n"
        "\n"
        "Internally displaced persons are defined according to the 1998 "
        "Guiding Principles "
        "(https://www.internal-displacement.org/publications/ocha-guiding-principles-on-internal-displacement) "
        "as people or groups of people who have been forced or obliged to "
        "flee or to leave their homes or places of habitual residence, in "
        "particular as a result of armed conflict, or to avoid the effects "
        "of armed conflict, situations of generalized violence, violations "
        "of human rights, or natural or human-made disasters and who have "
        "not crossed an international border.\n"
        "\n"
        "The IDMC's Event data, sourced from the Internal Displacement "
        "Updates (IDU), offers initial assessments of internal displacements "
        "reported within the last 180 days. This dataset provides "
        "provisional information that is continually updated on a daily "
        "basis, reflecting the availability of data on new displacements "
        "arising from conflicts and disasters. The finalized, carefully "
        "curated, and validated estimates are then made accessible through "
        "the Global Internal Displacement Database (GIDD), accessible at "
        "https://www.internal-displacement.org/database/displacement-data. The "
        "IDU dataset comprises preliminary estimates aggregated from various "
        "publishers or sources.\n",
        "owner_org": "647d9d8c-4cac-4c33-b639-649aad1c2893",
        "subnational": "0",
        "tags": [
            {
                "name": "conflict-violence",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {
                "name": "cyclones-hurricanes-typhoons",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {
                "name": "natural disasters",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {
                "name": "flooding-storm surge",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {
                "name": "displacement",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {"name": "hxl", "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87"},
            {
                "name": "internally displaced persons-idp",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
        ],
        "title": "India - IDMC Event data (Internal Displacement Updates)",
    }
    ind_resource = {
        "description": "idmc event data for IND for India",
        "format": "csv",
        "name": "idmc event data for IND",
        "resource_type": "file.upload",
        "url_type": "upload",
    }
    ind_showcase = {
        "image_url": "https://www.internal-displacement.org/sites/default/files/logo_0.png",
        "name": "idmc-event-data-for-ind-showcase",
        "notes": "Click the image to go to the IDMC summary page for the India "
        "dataset",
        "tags": [
            {
                "name": "conflict-violence",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {
                "name": "cyclones-hurricanes-typhoons",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {
                "name": "natural disasters",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {
                "name": "flooding-storm surge",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {
                "name": "displacement",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {"name": "hxl", "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87"},
            {
                "name": "internally displaced persons-idp",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
        ],
        "title": "IDMC India Summary Page",
        "url": "http://www.internal-displacement.org/countries/India/",
    }

    afg_dataset = {
        "data_update_frequency": "1",
        "dataset_date": "[2023-05-18T00:00:00 TO 2023-11-14T23:59:59]",
        "groups": [{"name": "afg"}],
        "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
        "name": "idmc-event-data-for-afg",
        "notes": "**Resource has no data rows!** No conflict and disaster population "
        "movement (flows) data recorded for Afghanistan in the last 180 "
        "days.\n"
        "\n"
        "Internally displaced persons are defined according to the 1998 "
        "Guiding Principles "
        "(https://www.internal-displacement.org/publications/ocha-guiding-principles-on-internal-displacement) "
        "as people or groups of people who have been forced or obliged to "
        "flee or to leave their homes or places of habitual residence, in "
        "particular as a result of armed conflict, or to avoid the effects "
        "of armed conflict, situations of generalized violence, violations "
        "of human rights, or natural or human-made disasters and who have "
        "not crossed an international border.\n"
        "\n"
        "The IDMC's Event data, sourced from the Internal Displacement "
        "Updates (IDU), offers initial assessments of internal displacements "
        "reported within the last 180 days. This dataset provides "
        "provisional information that is continually updated on a daily "
        "basis, reflecting the availability of data on new displacements "
        "arising from conflicts and disasters. The finalized, carefully "
        "curated, and validated estimates are then made accessible through "
        "the Global Internal Displacement Database (GIDD), accessible at "
        "https://www.internal-displacement.org/database/displacement-data. "
        "The IDU dataset comprises preliminary estimates aggregated from "
        "various publishers or sources.\n",
        "owner_org": "647d9d8c-4cac-4c33-b639-649aad1c2893",
        "subnational": "0",
        "tags": [
            {
                "name": "displacement",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {"name": "hxl", "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87"},
            {
                "name": "internally displaced persons-idp",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
        ],
        "title": "Afghanistan - IDMC Event data (Internal Displacement Updates)",
    }

    afg_resource = {
        "name": "idmc event data for AFG",
        "description": "idmc event data for AFG for Afghanistan  \n**Resource has no data rows!**",
        "format": "csv",
        "resource_type": "file.upload",
        "url_type": "upload",
    }

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        UserAgent.set_global("test")
        Locations.set_validlocations(
            [{"name": "afg", "title": "Afghanistan"}, {"name": "ind", "title": "India"}]
        )
        Country.countriesdata(use_live=False)
        tags = (
            "conflict-violence",
            "cyclones-hurricanes-typhoons",
            "displacement",
            "flooding-storm surge",
            "hxl",
            "internally displaced persons-idp",
            "natural disasters",
        )
        Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
        Vocabulary._tags_dict["conflict"] = {
            "Action to Take": "merge",
            "New Tag(s)": "conflict-violence",
        }
        Vocabulary._tags_dict["cyclone"] = {
            "Action to Take": "merge",
            "New Tag(s)": "cyclones-hurricanes-typhoons",
        }
        Vocabulary._tags_dict["erosion"] = {
            "Action to Take": "merge",
            "New Tag(s)": "natural disasters",
        }
        Vocabulary._tags_dict["flood"] = {
            "Action to Take": "merge",
            "New Tag(s)": "flooding-storm surge",
        }
        tags = [{"name": tag} for tag in tags]
        Vocabulary._approved_vocabulary = {
            "tags": tags,
            "id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            "name": "approved",
        }
        return Configuration.read()

    def test_generate_dataset_and_showcase(self, configuration, fixtures):
        with temp_dir(
            "test_idmc", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(downloader, folder, fixtures, folder, False, True)
                # indicator dataset test
                today = parse_date("2023-11-14")
                idmc = IDMC(configuration, retriever, today, folder)
                idmc.get_idmc_territories()
                countries = idmc.get_countriesdata()
                assert len(countries) == 167

                (
                    dataset,
                    showcase,
                    show_quickcharts,
                ) = idmc.generate_dataset_and_showcase("IND")
                assert dataset == self.ind_dataset
                resources = dataset.get_resources()
                assert resources[0] == self.ind_resource
                file = "event_data_IND.csv"
                assert_files_same(join("tests", "fixtures", file), join(folder, file))
                assert showcase == self.ind_showcase
                assert show_quickcharts is True

                (
                    dataset,
                    showcase,
                    show_quickcharts,
                ) = idmc.generate_dataset_and_showcase("AFG")
                assert dataset == self.afg_dataset
                resources = dataset.get_resources()
                assert resources[0] == self.afg_resource
                file = "event_data_AFG.csv"
                assert_files_same(join("tests", "fixtures", file), join(folder, file))
                assert showcase is None
                assert show_quickcharts is False
