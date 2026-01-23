from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.scraper.idmc.idu.pipeline import Pipeline
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.useragent import UserAgent


@pytest.fixture(scope="session")
def fixtures():
    return join("tests", "fixtures")


@pytest.fixture(scope="session")
def configuration():
    UserAgent.set_global("test")
    Configuration._create(
        hdx_read_only=True,
        hdx_site="prod",
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), Pipeline
        ),
    )
    Locations.set_validlocations(
        [{"name": "afg", "title": "Afghanistan"}, {"name": "ind", "title": "India"}]
    )
    Country.countriesdata(use_live=False)
    tags = (
        "conflict-violence",
        "cyclones-hurricanes-typhoons",
        "displacement",
        "flooding-storm surge",
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
