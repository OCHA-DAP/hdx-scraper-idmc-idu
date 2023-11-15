#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from copy import deepcopy
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import iso_string_from_datetime, now_utc, parse_date
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
                countries = idmc.get_countriesdata(state_dict)
                logger.info(f"Number of country datasets to upload: {len(countries)}")

                for _, nextdict in progress_storing_folder(info, countries, "iso3"):
                    countryiso = nextdict["iso3"]
                    dataset, showcase = idmc.generate_dataset_and_showcase(countryiso)
                    if dataset:
                        dataset.update_from_yaml()
                        dataset["notes"] = dataset["notes"].replace(
                            "\n", "  \n"
                        )  # ensure markdown has line breaks
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            hxl_update=False,
                            updated_by_script="HDX Scraper: IDMC IDU",
                            batch=batch,
                        )
                        dataset.generate_quickcharts()
                        if showcase:
                            showcase.create_in_hdx()
                            showcase.add_dataset(dataset)
        state.set(state_dict)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
