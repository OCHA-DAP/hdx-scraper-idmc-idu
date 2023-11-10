#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from copy import deepcopy
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import parse_date, iso_string_from_datetime, \
    now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch
from hdx.utilities.retriever import Retrieve
from hdx.utilities.state import State

from idmc import IDMC

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-idmc-idu"


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """

    with State(
        "country_dates.txt",
        State.dates_str_to_country_date_dict,
        State.country_date_dict_to_dates_str,
    ) as state:
        state_dict = deepcopy(state.get())
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info["folder"]
            with Download(
                extra_params_yaml=join(expanduser("~"), ".extraparams.yml"),
                extra_params_lookup=lookup,
            ) as downloader:
                retriever = Retrieve(
                    downloader, folder, "saved_data", folder, save, use_saved
                )
                folder = info["folder"]
                batch = info["batch"]
                configuration = Configuration.read()
                idmc = IDMC(configuration, retriever, folder)
                countries = idmc.get_country_data(state_dict)
                logger.info(f"Number of country datasets to upload: {len(countries)}")

                for _, nextdict in progress_storing_folder(info, countries, "iso3"):
                    countryiso = nextdict["iso3"]
                    (
                        dataset,
                        showcase,
                        bites_disabled,
                    ) = idmc.generate_dataset_and_showcase(
                        countryiso
                    )
                    if dataset:
                        dataset.update_from_yaml()
                        dataset.generate_quickcharts(bites_disabled=bites_disabled)
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            hxl_update=False,
                            updated_by_script="HDX Scraper: IDMC IDU",
                            batch=batch,
                        )
                        resources = dataset.get_resources()
                        resource_ids = [
                            x["id"]
                            for x in sorted(
                                resources, key=lambda x: len(x["name"])
                            )
                        ]
                        dataset.reorder_resources(resource_ids, hxl_update=False)


if __name__ == "__main__":
    facade(
        main,
        hdx_site="feature",
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
