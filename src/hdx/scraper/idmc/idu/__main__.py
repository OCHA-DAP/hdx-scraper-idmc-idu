#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os import getenv
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.scraper.idmc.idu._version import __version__
from hdx.scraper.idmc.idu.pipeline import Pipeline
from hdx.utilities.dateparse import now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

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

    logger.info(f"##### {lookup} version {__version__} ####")
    configuration = Configuration.read()
    User.check_current_user_write_access(
        "647d9d8c-4cac-4c33-b639-649aad1c2893", configuration=configuration
    )
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        idmc_key = getenv("IDMC_KEY")
        if idmc_key:
            extra_params_dict = {"client_id": idmc_key}
        else:
            extra_params_dict = None
        with Download(
            extra_params_dict=extra_params_dict,
            extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
            extra_params_lookup=lookup,
        ) as downloader:
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved
            )
            batch = info["batch"]
            today = now_utc()
            pipeline = Pipeline(configuration, retriever, today, folder)
            pipeline.get_idmc_territories()
            countries = pipeline.get_countriesdata()
            logger.info(f"Number of country datasets to upload: {len(countries)}")

            for _, nextdict in progress_storing_folder(info, countries, "iso3"):
                countryiso = nextdict["iso3"]
                (
                    dataset,
                    showcase,
                    show_quickcharts,
                ) = pipeline.generate_dataset_and_showcase(countryiso)
                if dataset:
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), main
                        )
                    )
                    dataset["notes"] = dataset["notes"].replace(
                        "\n", "  \n"
                    )  # ensure markdown has line breaks
                    if show_quickcharts:
                        dataset.generate_quickcharts(
                            path=script_dir_plus_file(
                                join("config", "hdx_resource_view_static.yaml"), main
                            ),
                        )
                    else:
                        dataset.preview_off()
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script="HDX Scraper: IDMC IDU",
                        batch=batch,
                    )
                    if showcase:
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
