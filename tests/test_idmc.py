#!/usr/bin/python
"""
Unit tests for IDMC

"""

from os.path import join

from hdx.scraper.idmc.idu.pipeline import Pipeline
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve


class TestIDMC:
    ind_dataset = {
        "data_update_frequency": "1",
        "dataset_date": "[2023-05-18T00:00:00 TO 2023-11-14T23:59:59]",
        "groups": [{"name": "ind"}],
        "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
        "name": "idmc-event-data-for-ind",
        "notes": "Conflict and disaster population movement (flows) data for India. \n"
        "\n"
        "The **IDU (Internal Displacement Updates) dataset**, provided by "
        "the [Internal Displacement Monitoring Centre "
        "(IDMC)](https://www.internal-displacement.org/), offers timely "
        "event data and provisional information on new internal "
        "displacements caused by conflicts and disasters. Representing the "
        "most recent available information over a 180-day time period, the "
        'IDU is updated daily and focuses on "flows" (new displacements).\n'
        "\n"
        "Internally displaced persons (IDPs) are defined according to the "
        "[1998 Guiding "
        "Principles](https://www.internal-displacement.org/internal-displacement/guiding-principles-on-internal-displacement/) "
        "as people or groups of people who have been forced or obliged to "
        "flee or to leave their homes or places of habitual residence, in "
        "particular as a result of armed conflict, or to avoid the effects "
        "of armed conflict, situations of generalized violence, violations "
        "of human rights, or natural or human-made disasters and who have "
        "not crossed an international border. The IDMC's event data, sourced "
        "from the IDU, provides initial assessments of these internal "
        "displacements, reflecting continually updated provisional "
        "information from various sources.\n"
        "\n"
        "While the IDU offers early insights, the more thoroughly validated "
        'and curated "stock"  (Total number of people leaving on internal '
        'displacement) and "flow" (population movements) estimates are '
        "available in the annual [Global Internal Displacement Database "
        "(GIDD)](http://www.internal-displacement.org/database/displacement-data). "
        "Both datasets are accessible via API, with specific guidance on "
        "data access, structure, and limitations, including important "
        "preprocessing considerations for the IDU to ensure accurate "
        "analysis and avoid double-counting. For further detailed "
        "information and complete API specifications, users are encouraged "
        "to consult the official documentation at "
        "https://www.internal-displacement.org/database/api-documentation/.\n"
        "\n"
        "The IDMC's Event data, sourced from the Internal Displacement "
        "Updates (IDU), offers initial assessments of internal displacements "
        "reported within the last 180 days. This dataset provides "
        "provisional information that is continually updated on a daily "
        "basis, reflecting the availability of data on new displacements "
        "arising from conflicts and disasters. The finalized, carefully "
        "curated, and validated estimates are then made accessible through "
        "[the Global Internal Displacement Database "
        "(GIDD)](https://www.internal-displacement.org/database/displacement-data). "
        "The IDU dataset comprises preliminary estimates aggregated from "
        "various publishers or sources.\n",
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
            {
                "name": "internally displaced persons-idp",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
        ],
        "title": "India - Internal Displacements Updates (IDU) (event data)",
    }
    ind_resource = {
        "description": "idmc event data for India",
        "format": "csv",
        "name": "idmc event data for IND",
    }
    ind_showcase = {
        "image_url": "https://www.internal-displacement.org/sites/default/files/logo_0.png",
        "name": "idmc-event-data-for-ind-showcase",
        "notes": "Click the image to go to the IDMC summary page for the India dataset",
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
        "The **IDU (Internal Displacement Updates) dataset**, provided by "
        "the [Internal Displacement Monitoring Centre "
        "(IDMC)](https://www.internal-displacement.org/), offers timely "
        "event data and provisional information on new internal "
        "displacements caused by conflicts and disasters. Representing the "
        "most recent available information over a 180-day time period, the "
        'IDU is updated daily and focuses on "flows" (new displacements).\n'
        "\n"
        "Internally displaced persons (IDPs) are defined according to the "
        "[1998 Guiding "
        "Principles](https://www.internal-displacement.org/internal-displacement/guiding-principles-on-internal-displacement/) "
        "as people or groups of people who have been forced or obliged to "
        "flee or to leave their homes or places of habitual residence, in "
        "particular as a result of armed conflict, or to avoid the effects "
        "of armed conflict, situations of generalized violence, violations "
        "of human rights, or natural or human-made disasters and who have "
        "not crossed an international border. The IDMC's event data, sourced "
        "from the IDU, provides initial assessments of these internal "
        "displacements, reflecting continually updated provisional "
        "information from various sources.\n"
        "\n"
        "While the IDU offers early insights, the more thoroughly validated "
        'and curated "stock"  (Total number of people leaving on internal '
        'displacement) and "flow" (population movements) estimates are '
        "available in the annual [Global Internal Displacement Database "
        "(GIDD)](http://www.internal-displacement.org/database/displacement-data). "
        "Both datasets are accessible via API, with specific guidance on "
        "data access, structure, and limitations, including important "
        "preprocessing considerations for the IDU to ensure accurate "
        "analysis and avoid double-counting. For further detailed "
        "information and complete API specifications, users are encouraged "
        "to consult the official documentation at "
        "https://www.internal-displacement.org/database/api-documentation/.\n"
        "\n"
        "The IDMC's Event data, sourced from the Internal Displacement "
        "Updates (IDU), offers initial assessments of internal displacements "
        "reported within the last 180 days. This dataset provides "
        "provisional information that is continually updated on a daily "
        "basis, reflecting the availability of data on new displacements "
        "arising from conflicts and disasters. The finalized, carefully "
        "curated, and validated estimates are then made accessible through "
        "[the Global Internal Displacement Database "
        "(GIDD)](https://www.internal-displacement.org/database/displacement-data). "
        "The IDU dataset comprises preliminary estimates aggregated from "
        "various publishers or sources.\n",
        "owner_org": "647d9d8c-4cac-4c33-b639-649aad1c2893",
        "subnational": "0",
        "tags": [
            {
                "name": "displacement",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
            {
                "name": "internally displaced persons-idp",
                "vocabulary_id": "4e61d464-4943-4e97-973a-84673c1aaa87",
            },
        ],
        "title": "Afghanistan - Internal Displacements Updates (IDU) (event data)",
    }

    afg_resource = {
        "name": "idmc event data for AFG",
        "description": "idmc event data for Afghanistan  \n**Resource has no data rows!**",
        "format": "csv",
    }

    def test_generate_dataset_and_showcase(self, configuration, fixtures):
        with temp_dir(
            "test_idmc", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(downloader, folder, fixtures, folder, False, True)
                # indicator dataset test
                today = parse_date("2023-11-14")
                pipeline = Pipeline(configuration, retriever, today, folder)
                pipeline.get_idmc_territories()
                countries = pipeline.get_countriesdata()
                assert len(countries) == 167

                (
                    dataset,
                    showcase,
                ) = pipeline.generate_dataset_and_showcase("IND")
                assert dataset == self.ind_dataset
                resources = dataset.get_resources()
                assert resources[0] == self.ind_resource
                file = "event_data_IND.csv"
                assert_files_same(join(fixtures, file), join(folder, file))
                assert showcase == self.ind_showcase

                (
                    dataset,
                    showcase,
                ) = pipeline.generate_dataset_and_showcase("AFG")
                assert dataset == self.afg_dataset
                resources = dataset.get_resources()
                assert resources[0] == self.afg_resource
                file = "event_data_AFG.csv"
                assert_files_same(join(fixtures, file), join(folder, file))
                assert showcase is None
