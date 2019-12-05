# Copyright 2018-2019 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
#     or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.

"""Application entry point."""
import getpass
import glob
from pathlib import Path
from typing import Iterable, Type, Dict, Union

from kedro.context import KedroContext, load_context
from kedro.io import DataCatalog
from kedro.runner import AbstractRunner
from kedro.pipeline import Pipeline

from kedro.contrib.config import TemplatedConfigLoader
from pyspark import SparkConf

from homelesslink.pipeline import create_pipelines
from kedro.versioning import Journal

from pyspark.sql import SparkSession

import logging

logger = logging.getLogger(__name__)


class ProjectContext(KedroContext):
    """Users can override the remaining methods from the parent class here, or create new ones
    (e.g. as required by plugins)

    """

    project_name = "homelesslink"
    project_version = "0.15.4"

    def __init__(self, project_path: Union[Path, str], env: str = None):
        super().__init__(project_path, env)
        self._spark = None
        self.init_spark_session()

    def init_spark_session(self, yarn=False) -> None:
        """Initialises a SparkSession using the config defined in project's conf folder."""

        if self._spark:
            return self._spark
        parameters = self.config_loader.get("spark*", "spark*/**")
        logger.info("parameters: {}".format(parameters))
        spark_conf = SparkConf().setAll(parameters.items())

        spark_session_conf = (
            SparkSession.builder.appName(
                "{}_{}".format(self.project_name, getpass.getuser())
            )
            .enableHiveSupport()
            .config(conf=spark_conf)
        )
        if yarn:
            self._spark = spark_session_conf.master("yarn").getOrCreate()
        else:
            self._spark = spark_session_conf.master("local[*]").getOrCreate()

        logger.info("** Spark session created with following config:")
        for item in sorted(self._spark.sparkContext._conf.getAll()):
            logging.info(item)
        logger.info(
            "SparkUI at: {spark_ui}".format(
                spark_ui=self._spark._jsc.sc().uiWebUrl().get()
            )
        )

        self._spark.sparkContext.setLogLevel("WARN")

    def _get_config_loader(self) -> TemplatedConfigLoader:
        """A hook for changing the creation of a ConfigLoader instance.

        Returns:
            Instance of `ConfigLoader` created by `_create_config_loader()`.

        """

        conf_paths = [
            str(self.project_path / self.CONF_ROOT / "base"),
            str(self.project_path / self.CONF_ROOT / self.env),
            str(self.project_path / self.CONF_ROOT),
        ] + [
            str(self.project_path / x)
            for x in glob.glob("**/pipelines/*/conf", recursive=True)
        ]

        return self._create_config_loader(conf_paths)

    def _create_config_loader(self, conf_paths: Iterable[str]) -> TemplatedConfigLoader:
        return TemplatedConfigLoader(conf_paths, globals_pattern="*globals.yml",)

    def _get_pipelines(self) -> Dict[str, Pipeline]:
        return create_pipelines()

    def _get_catalog(
        self,
        save_version: str = None,
        journal: Journal = None,
        load_versions: Dict[str, str] = None,
    ) -> DataCatalog:
        """A hook for changing the creation of a DataCatalog instance.

        Returns:
            DataCatalog defined in `catalog.yml`.

        """
        conf_catalog = self.config_loader.get("catalog*", "catalog*/**")
        conf_creds = self._get_config_credentials()
        catalog = self._create_catalog(
            conf_catalog, conf_creds, save_version, journal, load_versions
        )
        catalog.add_feed_dict(self._get_feed_dict())
        return catalog


def main(
    tags: Iterable[str] = None,
    env: str = None,
    runner: Type[AbstractRunner] = None,
    node_names: Iterable[str] = None,
    from_nodes: Iterable[str] = None,
    to_nodes: Iterable[str] = None,
    from_inputs: Iterable[str] = None,
):
    """Application main entry point.

    Args:
        tags: An optional list of node tags which should be used to
            filter the nodes of the ``Pipeline``. If specified, only the nodes
            containing *any* of these tags will be run.
        env: An optional parameter specifying the environment in which
            the ``Pipeline`` should be run.
        runner: An optional parameter specifying the runner that you want to run
            the pipeline with.
        node_names: An optional list of node names which should be used to filter
            the nodes of the ``Pipeline``. If specified, only the nodes with these
            names will be run.
        from_nodes: An optional list of node names which should be used as a
            starting point of the new ``Pipeline``.
        to_nodes: An optional list of node names which should be used as an
            end point of the new ``Pipeline``.
        from_inputs: An optional list of input datasets which should be used as a
            starting point of the new ``Pipeline``.

    """

    project_context = load_context(Path.cwd(), env=env)
    project_context.run(
        tags=tags,
        runner=runner,
        node_names=node_names,
        from_nodes=from_nodes,
        to_nodes=to_nodes,
        from_inputs=from_inputs,
    )


if __name__ == "__main__":
    main()
