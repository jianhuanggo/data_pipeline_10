import argparse
import json
import logging
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

import nbformat as nbf
import yaml
from dbt.cli.main import dbtRunner, dbtRunnerResult
from dbt.contracts.graph.manifest import Manifest
from jsonschema import ValidationError, validate

dbt = dbtRunner()
logging.basicConfig(level=logging.INFO)
redshift_jar_path = "dbfs:/vendor/RedshiftJDBC42.jar"
deltalake_jar_path = "s3://adrise-artifacts/adRise/sparky/deltalake/master/deltalake-assembly-0.0.41.jar"

parser = argparse.ArgumentParser()
parser.add_argument("--job_name", type=str)
parser.add_argument("--schedule", type=str)
parser.add_argument("--retries", type=int, default=0)
parser.add_argument("--wait_interval", type=int, default=60)
parser.add_argument("--wait_timeout", type=int, default=3 * 60 * 60)
parser.add_argument("--threads", type=int, default=0)
parser.add_argument("--tags", type=str, default="{}")
parser.add_argument("--sync_redshift", type=bool, default=False)
parser.add_argument("--enabled_notifications", type=bool, default=True)
parser.add_argument("--project", type=str)
parser.add_argument("--target", type=str)
parser.add_argument("--command", type=str)
parser.add_argument("--select", type=str, default="")
parser.add_argument("--selector", type=str, default="")
parser.add_argument("--exclude", type=str, default="")
parser.add_argument("--vars", nargs="?", const="", default="{}")
parser.add_argument("--args", nargs="?", const="", default="{}")
parser.add_argument("--cluster_key", type=str)
parser.add_argument("--full-refresh", type=bool, default=False)
parser.add_argument("--version", type=int, default=3)

parser.add_argument("--start", type=str)
parser.add_argument("--end", type=str)
parser.add_argument("--days", type=int)
parser.add_argument("--weeks", type=int)
parser.add_argument("--months", type=int)
# parser.add_argument("--branch", type=str)

args = parser.parse_args()




def validate_dbt_job_definition(job):
    file_path = Path(__file__).parent / "job_template.yml"
    schema = yaml.safe_load(file_path.read_text())
    try:
        validate(instance=job, schema=schema)
    except ValidationError as ex:
        logging.info(ex.schema["error_msg"] if "error_msg" in ex.schema else ex.message)
        raise
    else:
        logging.info("Valid Job YAML File!")


def load_job_definition(job_definition_file, dbt_dir=""):
    """Load tubibricks.yml"""
    local_filepath = Path(f"{dbt_dir}/{job_definition_file}")

    print(local_filepath)

    try:
        return yaml.safe_load(local_filepath.read_text())
    except Exception as err:
        return yaml.safe_load(Path(job_definition_file).read_text())


def create_src_dir(src_dir):
    """Makes sure a nested directory exists"""
    if not os.path.exists(src_dir):
        os.makedirs(src_dir)


def date_range(start_date, end_date, duration, interval):
    """Creates multiple sub-intervals for a date range"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = relativedelta(**{interval: duration})
    ranges = []
    from_date = start
    while from_date < end:
        to_date = from_date + delta
        if to_date > end:
            to_date = end
        ranges.append((from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")))
        from_date = to_date

    return ranges


allowed_intervals = ["days", "weeks", "months"]


def parse_range_args(start_date, end_date, range_args):
    if start_date is None or end_date is None:
        return []

    for i in allowed_intervals:
        if i in range_args and range_args[i] is not None:
            duration = range_args[i]
            interval = i
            return date_range(start_date, end_date, duration, interval)

    raise ValueError(f"The following intervals are supported: {', '.join(allowed_intervals)}.")


allowed_versions = [2, 3]


def version_error(version):
    return f"Not allowed 'version' argument {version}. Use one of: {', '.join(map(str, allowed_versions))}."


def safe_load_json(myjson):
    """Parses JSON string"""
    try:
        return json.loads(myjson)
    except ValueError:
        return None


def inject_params_into_script(script_content, params):
    for param_name, param_value in params.items():
        placeholder = f"{{{param_name}}}"
        if placeholder in script_content:
            script_content = script_content.replace(placeholder, str(param_value))

    return script_content


class DbtTask:
    """Generates notebooks for each task using manifest dependencies"""

    def __init__(
        self,
        dbt_dir,
        job_name,
        job_cluster_key,
        dbt_project,
        dbt_target,
        dbt_command,
        dbt_flags,
        dbt_select,
        dbt_selector,
        dbt_exclude,
        dbt_vars="{}",
        dbt_args="{}",
        task_retries=0,
        task_wait_interval=0,
        task_wait_timeout=0,
        threads=0,
    ):
        self.dbt_dir = dbt_dir
        self.job_name = job_name
        self.manifest_data = Manifest()
        self.job_cluster_key = job_cluster_key
        self.dbt_project = dbt_project
        self.dbt_target = dbt_target
        self.dbt_cmd = "model" if dbt_command == "run" else dbt_command
        self.dbt_flags = dbt_flags
        self.dbt_select = dbt_select
        self.dbt_exclude = dbt_exclude
        self.dbt_vars = dbt_vars
        self.dbt_args = dbt_args
        self.task_retries = task_retries
        self.task_wait_interval = task_wait_interval
        self.task_wait_timeout = task_wait_timeout
        self.threads = threads
        self.model_select = dbt_select if dbt_command != "run-operation" and dbt_select else dbt_project
        self.model_selector = dbt_selector
        self.model_exclude = dbt_exclude
        self.selected_nodes = []
        self.compiled_jinja = {}
        self.semaphore_template = None

    def dbt_setup(self):
        """Installs dbt dependencies for the project"""
        cli_args = ["deps"] + ["--target", self.dbt_target] + ["--project-dir", self.dbt_dir]
        res: dbtRunnerResult = dbt.invoke(cli_args)
        if not res.success:
            logging.error(res.exception)

    def load_manifest(self):
        """Load manifest.json"""
        cli_args = ["parse"] + ["--target", self.dbt_target]
        res: dbtRunnerResult = dbtRunner().invoke(cli_args)
        if not res.success:
            logging.error(res.exception)

        self.manifest_data: Manifest = res.result
        return self.manifest_data

    def dbt_run_command(self, command="run", is_quiet=False):
        """Runs dbt command"""
        cli_args = (
            [command]
            + self.dbt_flags
            + (["--selector", self.model_selector] if self.model_selector > "" else ["--select", self.model_select])
            + (["--exclude", self.model_exclude] if self.model_exclude > "" else [])
            + ["--vars", f"""{self.dbt_vars}"""]
            + ["--target", self.dbt_target]
        )
        if is_quiet:
            cli_args.insert(0, "--quiet")
        res: dbtRunnerResult = dbt.invoke(cli_args)
        if not res.success:
            logging.error(res.exception)

        self.load_manifest()
        return self.manifest_data

    def dbt_compile(self):
        """Compiles dbt project"""
        return self.dbt_run_command("compile")

    def parse_model_selector(self):
        """Run the dbt ls command which returns all dbt models associated with a particular selection syntax"""
        cli_args = (
            ["--quiet", "ls"]
            + ["--resource-type", self.dbt_cmd]
            + (["--selector", self.model_selector] if self.model_selector > "" else ["--select", self.model_select])
            + (["--exclude", self.model_exclude] if self.model_exclude > "" else [])
            + ["--target", self.dbt_target]
        )
        res: dbtRunnerResult = dbt.invoke(cli_args)
        if not res.success:
            logging.error(res.exception)

        models = res.result
        if len(models) == 0:
            exclude_log = f"with exclude '{self.model_exclude}' " if self.model_exclude else ""
            raise ValueError(
                f"The selection criterion '{self.dbt_selector or self.dbt_select}' {exclude_log}does not match any nodes. No nodes selected!"
            )

        return models

    def reformat_node(self, node):
        """Change the format of the node name to match manifest.json"""
        split_node = node.split(".")
        length_split_node = len(split_node)
        node = split_node[0] + "." + split_node[length_split_node - 1]
        node = f"{self.dbt_cmd}.{node}"
        return node

    def format_manifest_nodes(self, models):
        """Clean up the naming of the nodes so they match the structure what
        is coming out generate_all_model_dependencies function. This function doesn't create
        a list of dependencies between selected nodes (that happens in generate_task_dependencies)
        it's just cleaning up the naming of the nodes and outputting them as a list"""
        manifest_nodes = []
        for node in models:
            node = self.reformat_node(node)
            manifest_nodes.append(node)
        return manifest_nodes

    def read_compiled_code(self, package_name, model_path):
        """Reads compiled dbt code from a file"""
        compiled_path = f"target/compiled/{package_name}/models/{model_path}"
        compiled_code_wrapper = open(compiled_path, "r")
        return compiled_code_wrapper.read()

    def compile_inline(self, jinja, model_alias = None):
        """Compiles a dbt jinja, caching it in a variable to avoid compiling the same hooks multiple times"""
        if jinja not in self.compiled_jinja:
            logging.debug(jinja)
            cli_args = (
                ["--quiet", "compile"]
                + ["--vars", f"""{self.dbt_vars}"""]
                + ["--target", self.dbt_target]
                + ["--inline", f"""{jinja}"""]
            )
            res: dbtRunnerResult = dbt.invoke(cli_args)
            if not res.success:
                logging.error(res.exception)

            result = ""
            execution_result = res.result
            run_results = execution_result.results
            for run_result in run_results:
                result += run_result.node.compiled_code

            if len(run_results) == 0:
                raise ValueError(f'Failed to compile jinja: "{jinja}".')

            # Handle {{ this }} in hooks incorrectly being converted to `inline_query`
            if model_alias:
                inline_query_this = ".`inline_query`"
                result = result.replace(inline_query_this, f".`{model_alias}`")

            self.compiled_jinja[jinja] = result

        return self.compiled_jinja[jinja]

    def get_subdirectories_path(self, fqn, resource_type=None):
        """Creates a directory path based on fqn list from the manifest"""
        subdirectories = [self.job_name] if self.job_name else []
        subdirectories.append(f"{resource_type}s" if resource_type else "macros")
        # dbt-databricks-dry doesn't track node_path for tests despite it's available in manifest
        if resource_type != "test":
            last_index = -1
            if resource_type == "snapshot":
                last_index = -2
            subdirectories += fqn[1:last_index]

        return "/".join(subdirectories)

    def create_notebook(self, subdirectories_path, model_name, cells):
        """Creates or replaces an SQL notebook compatible with Databricks"""
        nb = nbf.v4.new_notebook()
        nb.metadata = {"application/vnd.databricks.v1+notebook": {"language": "sql"}}
        nb["cells"] = cells
        create_src_dir(f"src/{subdirectories_path}")
        nbf.write(nb, self.task_notebook_path(model_name, subdirectories_path))

    def get_node_id(self, node, manifest_nodes):
        """Looks for a node_id by it's prefix. Tests often have a hash value in the end of the key that we need to find"""
        if node in manifest_nodes:
            return node

        for manifest_node in manifest_nodes.keys():
            if manifest_node.startswith(node):
                return manifest_node

        raise ValueError(f"Couldn't find '{node}' node.")

    def task_notebook_path(self, model_name, subdirectories_path):
        """Generates standard notebook file path"""
        return f"src/{subdirectories_path}/{model_name}.ipynb"

    def format_task_key(self, model_name, task_suffix=None):
        """Formats dependency or model name task key"""
        task_key = f"{model_name}_{task_suffix}" if task_suffix else model_name
        return task_key

    def add_semaphore_cells(self, notebook_path, python_code):
        nb = nbf.read(notebook_path, nbf.NO_CONVERT)
        code = "%python\n" + python_code
        nb["cells"].insert(0, nbf.v4.new_code_cell(code))
        nb["cells"].insert(0, nbf.v4.new_code_cell("%python\ndbutils.library.restartPython()"))
        nb["cells"].insert(0, nbf.v4.new_code_cell("%python\n%pip install -U databricks-sdk"))
        nbf.write(nb, notebook_path)

    def inject_semaphore(self, notebook_path):
        """Injects semaphore logic into the notebook"""
        if self.threads > 0 and len(self.selected_nodes) > self.threads:
            semaphore_params = {
                "notebook_path": notebook_path,
                "timeout": self.task_wait_timeout,
                "threads": self.threads,
                "interval": self.task_wait_interval,
            }
            semaphore_trigger = inject_params_into_script(self.semaphore_template, semaphore_params)
            self.add_semaphore_cells(notebook_path, semaphore_trigger)

    def generate(self, task_suffix=None, run_after=[]):
        """Creates a list of tasks for each node with their corresponding dependencies"""
        self.selected_nodes = self.compile_nodes()
        manifest_nodes = self.manifest_data.nodes
        tasks = []
        default_dependencies = []
        for task_key in run_after:
            default_dependencies.append({"task_key": task_key})

        if not self.semaphore_template and self.threads > 0:
            semaphore_template_file_path = Path("../scripts/trigger_threads_template.py")
            self.semaphore_template = semaphore_template_file_path.read_text()

        for node in self.selected_nodes:
            node_id = self.get_node_id(node, manifest_nodes)
            node_details = manifest_nodes[node_id]
            dependencies = []
            if hasattr(node_details.depends_on, "nodes"):
                dependencies = node_details.depends_on.nodes
            resource_type = node_details.resource_type
            subdirectories_path = self.get_subdirectories_path(node_details.fqn, resource_type)
            model_cluster_key = node_details.config._extra.get("cluster_key")
            task_key = self.format_task_key(node_details.name, task_suffix)
            file_name = self.format_task_key(node_details.alias, task_suffix)
            task_notebook_path = self.task_notebook_path(file_name, subdirectories_path)

            task = {
                "task_key": task_key,
                "job_cluster_key": model_cluster_key or self.job_cluster_key,
                "notebook_task": {"notebook_path": "../" + task_notebook_path},
            }
            if self.task_retries > 0:
                task["max_retries"] = self.task_retries

            depends_on = default_dependencies.copy()
            for dependency in dependencies:
                if dependency in self.selected_nodes:
                    dependency_name = dependency.split(".")[-1]
                    task_key = self.format_task_key(dependency_name, task_suffix)
                    depends_on.append({"task_key": task_key})

            if len(depends_on) > 0:
                task["depends_on"] = depends_on

            self.inject_semaphore(task_notebook_path)
            tasks.append(task)

        return tasks

    def compile_nodes(self):
        """Applicable for model and tests commands"""
        selected_models = self.parse_model_selector()
        return self.format_manifest_nodes(selected_models)

    def dry_run(self):
        """Runs dbt using dbt-databricks-dry mode to generate notebooks"""
        manifest_data = self.execute(is_quiet=False)
        return self.format_manifest_nodes(manifest_data.nodes)


class DbtRunOperationTask(DbtTask):
    """dbt run-operation specific compilation flow"""

    def compile_inline_args(self):
        """Formats --args flag input into comma separated values"""
        compiled_args = ""
        if self.dbt_args:
            parsed_yaml = yaml.safe_load(self.dbt_args)
            yaml_args = []
            for k, v in parsed_yaml:
                yaml_args.append(f"{k}: {v}")

            compiled_args += ", ".join(yaml_args)

        return compiled_args

    def compile(self):
        """Compiles macro and uses it to generate notebook"""
        header = f"#{self.job_name}: {self.dbt_select}"
        inline_args = self.compile_inline_args()
        compiled_sql = self.compile_inline("{{ " + self.dbt_select + "(" + inline_args + ") }}")
        cells = [nbf.v4.new_markdown_cell(header), nbf.v4.new_code_cell(compiled_sql)]

        subdirectories_path = f"{self.job_name}/macros"
        macro_name = self.dbt_select.split(".")[-1]
        self.create_notebook(subdirectories_path, macro_name, cells)
        logging.info(f"'{self.dbt_select}' compiled successfully.")

    def execute(self, is_quiet=False):
        """Runs dbt macro"""
        cli_args = (
            ["run-operation", self.dbt_select]
            + ["--args", f"""{self.dbt_args}"""]
            + ["--vars", f"""{self.dbt_vars}"""]
            + ["--target", self.dbt_target]
        )
        if is_quiet:
            cli_args.insert(0, "--quiet")
        res: dbtRunnerResult = dbt.invoke(cli_args)

        if not res.success:
            logging.error(res.exception)

        self.load_manifest()
        return self.manifest_data

    def generate(self):
        """Creates a task for a single macro operation"""
        subdirectories_path = f"{self.job_name}/macros"
        macro_name = self.dbt_select.split(".")[-1]
        task_notebook_path = self.task_notebook_path(macro_name, subdirectories_path)

        task = {
            "task_key": macro_name,
            "job_cluster_key": self.job_cluster_key,
            "notebook_task": {"notebook_path": "../" + task_notebook_path},
        }
        if self.task_retries > 0:
            task["max_retries"] = self.task_retries

        self.inject_semaphore(task_notebook_path)
        return [task]


class DbtTestTask(DbtTask):
    """dbt test specific compilation flow"""

    def format_test_kwargs(self, kwargs):
        """Creates a jinja variable to be applied in a function downstream"""
        return "{% " + f"set _dbt_generic_test_kwargs = {kwargs}" + " %}"

    def wrap_test_compiled_code(self, compiled_code, test_name, severity, fail_calc, error_if):
        """Creates an SQL block based on a compiled dbt test"""
        operator = "" if severity.lower() != "error" else "ASSERT_TRUE"
        return f"""WITH {test_name} AS (
        {compiled_code}
        )
        SELECT {operator}(!({fail_calc} {error_if})) FROM {test_name}"""

    def test_cells(self, compiled_code, node_details):
        """Generates notebook code for dbt tests"""
        test_name = node_details.alias
        severity = node_details.config.severity
        fail_calc = node_details.config.fail_calc
        error_if = node_details.config.error_if
        run_code = self.wrap_test_compiled_code(compiled_code, test_name, severity, fail_calc, error_if)
        return [nbf.v4.new_code_cell(run_code)]

    def compile(self):
        """Compiles SQL to generate notebooks for each selected test"""
        self.selected_nodes = self.compile_nodes()
        manifest_nodes = self.manifest_data.nodes
        logging.info(f"Compiling {len(self.selected_nodes)} nodes.")
        for i, node in enumerate(self.selected_nodes):
            node_id = self.get_node_id(node, manifest_nodes)
            node_details = manifest_nodes[node_id]
            is_enabled = node_details.config.enabled
            if not is_enabled:
                logging.info(f"{i + 1}: Skipping '{node_id}'. It's not enabled.")
                continue

            resource_type = node_details.resource_type
            subdirectories_path = self.get_subdirectories_path(node_details.fqn, resource_type)
            test_name = node_details.name

            header = f"#{resource_type}: {test_name}"
            cells = [nbf.v4.new_markdown_cell(header)]

            raw_code = node_details.raw_code.replace("`", "").replace('"', "'")
            if type(node_details).__name__ == "GenericTestNode":
                kwargs = node_details.test_metadata.kwargs
                kwargs["model"] = self.compile_inline(kwargs["model"]).replace("`", "")
                test_kwargs = self.format_test_kwargs(kwargs)
                raw_code = test_kwargs + "\n" + raw_code

            compiled_sql = self.compile_inline(raw_code)
            cells += self.test_cells(compiled_sql, node_details)

            self.create_notebook(subdirectories_path, test_name, cells)
            logging.info(f"{i + 1}: '{node_id}' compiled successfully.")

    def execute(self, is_quiet=False):
        """Runs dbt test"""
        return self.dbt_run_command("test", is_quiet)


class DbtModelTask(DbtTask):
    """dbt model specific compilation flow"""

    jar_params = {
        "gen_symlink_manifest": "--gen-symlink-manifest",
        "add_new_partition": "--update-partition",
    }

    def wrap_incremental_compiled_code(self, compiled_code, tmp_model_name):
        """Creates a SQL block based on a compiled dbt code for incremental materialization"""
        return f"""CREATE OR REPLACE TEMPORARY VIEW {tmp_model_name} AS
        {compiled_code}"""

    def generate_replace_run_code(self, relation_name, tmp_model_name, partitions):
        sql = f"""CREATE OR REPLACE TABLE {relation_name} AS
          SELECT * FROM `{tmp_model_name}`"""
        if partitions:
            partition_columns = ', '.join(partitions)
            sql += f"""
            PARTITIONED BY ({partition_columns})"""

        return sql

    def generate_incremental_run_code(self, relation_name, tmp_model_name, unique_key):
        return f"""MERGE INTO {relation_name} AS INTERNAL_DEST USING `{tmp_model_name}` AS INTERNAL_SOURCE
          ON INTERNAL_SOURCE.{unique_key} = INTERNAL_DEST.{unique_key}
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *"""

    def incremental_cells(self, compiled_code, node_details):
        """Generates notebook code for `incremental` dbt materialization"""
        model_name = node_details.alias
        tmp_model_name = model_name + "__tmp"
        relation_name = node_details.relation_name
        unique_key = node_details.config.unique_key
        view_code = self.wrap_incremental_compiled_code(compiled_code, tmp_model_name)
        if "--full-refresh" in self.dbt_flags:
            partition_by = node_details.config.get("partition_by")
            partitions = [partition_by] if isinstance(partition_by, str) else partition_by
            run_code = self.generate_replace_run_code(relation_name, tmp_model_name, partitions)
        else:
            run_code = self.generate_incremental_run_code(relation_name, tmp_model_name, unique_key)

        return [nbf.v4.new_code_cell(view_code), nbf.v4.new_code_cell(run_code)]

    def wrap_table_compiled_code(self, compiled_code, relation_name):
        """Creates an SQL block based on a compiled dbt code for table materialization"""
        return f"""CREATE OR REPLACE TABLE {relation_name} USING DELTA AS
        {compiled_code}"""

    def table_cells(self, compiled_code, node_details):
        """Generates notebook code for `table` dbt materialization"""
        relation_name = node_details.relation_name
        run_code = self.wrap_table_compiled_code(compiled_code, relation_name)
        return [nbf.v4.new_code_cell(run_code)]

    def wrap_view_compiled_code(self, compiled_code, relation_name):
        """Creates an SQL block based on a compiled dbt code for view materialization"""
        return f"""CREATE OR REPLACE VIEW {relation_name} AS
        {compiled_code}"""

    def view_cells(self, compiled_code, node_details):
        """Generates notebook code for `view` dbt materialization"""
        relation_name = node_details.relation_name
        run_code = self.wrap_view_compiled_code(compiled_code, relation_name)
        return [nbf.v4.new_code_cell(run_code)]

    def model_materialization(self, materialized):
        materializations = {
            "incremental": self.incremental_cells,
            "table": self.table_cells,
            "view": self.view_cells,
        }
        if materialized in materializations:
            return materializations[materialized]

        logging.warning(f"{materialized} materialization not supported.")

    def application_task(self, jar_task_type, table_name):
        """Object for spark jar task"""
        return {
            "main_class_name": "tubi.spark.deltalake.Application",
            "parameters": [
                self.jar_params[jar_task_type],
                "--table",
                table_name,
                "--region",
                "us-west-2",
            ],
        }

    def sync_tasks(self, glue_db_name, app_cluster_key):
        """Creates tasks to generate symlink manifest and add glue partitions based on model tags"""
        manifest_nodes = self.manifest_data.nodes
        tasks = []
        for node in self.selected_nodes:
            node_id = self.get_node_id(node, manifest_nodes)
            node_details = manifest_nodes[node_id]
            model_name = node_details.name
            tags = node_details.config.tags
            main_task_name = self.format_task_key(model_name)
            table_name = f"{glue_db_name}.{model_name}"
            depends_on = main_task_name
            if "spectrum" in tags:
                jar_task_type = "gen_symlink_manifest"
                gen_symlink_manifest_task_name = f"{jar_task_type}_{main_task_name}"

                tasks.append(
                    {
                        "task_key": gen_symlink_manifest_task_name,
                        "job_cluster_key": app_cluster_key,
                        "libraries": [{"jar": redshift_jar_path}, {"jar": deltalake_jar_path}],
                        "spark_jar_task": self.application_task(jar_task_type, table_name),
                        "depends_on": [{"task_key": depends_on}],
                    }
                )
                depends_on = gen_symlink_manifest_task_name

            if "partitioned" in tags:
                jar_task_type = "add_new_partition"
                update_partition_task_name = f"{jar_task_type}_{main_task_name}"
                tasks.append(
                    {
                        "task_key": update_partition_task_name,
                        "job_cluster_key": app_cluster_key,
                        "libraries": [{"jar": redshift_jar_path}, {"jar": deltalake_jar_path}],
                        "spark_jar_task": self.application_task(jar_task_type, table_name),
                        "depends_on": [{"task_key": depends_on}],
                    }
                )

        return tasks

    def compile(self, task_suffix=None):
        """Uses compiled SQL to generate notebooks for each selected model"""
        self.selected_nodes = self.compile_nodes()
        manifest_nodes = self.manifest_data.nodes
        logging.info(f"Compiling {len(self.selected_nodes)} nodes.")
        for i, node in enumerate(self.selected_nodes):
            node_details = manifest_nodes[node]
            package_name = node_details.package_name
            model_path = node_details.path
            resource_type = node_details.resource_type
            subdirectories_path = self.get_subdirectories_path(node_details.fqn, resource_type)
            model_alias = node_details.alias
            model_name = node_details.name
            relation_name = node_details.relation_name
            materialized = node_details.config.materialized
            pre_hook = node_details.config.pre_hook
            post_hook = node_details.config.post_hook

            header = f"#{resource_type}: {relation_name}"
            compiled_code = self.read_compiled_code(package_name, model_path)
            cells = [nbf.v4.new_markdown_cell(header)]
            for hook in pre_hook:
                compiled_sql = self.compile_inline(hook.sql, model_alias)
                cells += [nbf.v4.new_code_cell(compiled_sql)]

            materialization = self.model_materialization(materialized)
            if materialization:
                cells += materialization(compiled_code, node_details)

            for hook in post_hook:
                compiled_sql = self.compile_inline(hook.sql, model_alias)
                cells += [nbf.v4.new_code_cell(compiled_sql)]

            task_name = self.format_task_key(model_name, task_suffix)
            self.create_notebook(subdirectories_path, task_name, cells)
            logging.info(f"{i + 1}: '{node}' compiled successfully.")

    def execute(self, is_quiet=False):
        """Runs dbt test"""
        return self.dbt_run_command("run", is_quiet)


class DbtSeedTask(DbtTask):
    """dbt seed specific compilation flow. Only supported on Tubibricks V3"""

    def execute(self, is_quiet=False):
        """Runs dbt seed"""
        return self.dbt_run_command("seed", is_quiet)


class DbtSnapshotTask(DbtTask):
    """dbt snapshot specific compilation flow. Only supported on Tubibricks V3"""

    def execute(self, is_quiet=False):
        """Runs dbt snapshot"""
        return self.dbt_run_command("snapshot", is_quiet)


dbt_commands = {"run-operation": DbtRunOperationTask, "test": DbtTestTask, "model": DbtModelTask, "run": DbtModelTask, "seed": DbtSeedTask, "snapshot": DbtSnapshotTask}


class DatabricksJob:
    """Workflow generation for Asset Bundles"""

    max_tasks_per_workflow = 500
    max_tests_per_workflow = 125

    def __init__(self, job_name, dbt_dir, dbt_command, manifest_data, target_resources, tasks, schedule, tags, timezone=None, is_notification_enabled=True):
        self.job_name = job_name
        self.dbt_dir = dbt_dir
        self.dbt_cmd = dbt_command
        self.manifest_data = manifest_data
        self.target_resources = target_resources
        self.tasks = tasks
        self.schedule = schedule
        self.tags = tags
        self.timezone = timezone or "UTC"
        self.is_notification_enabled = is_notification_enabled

    def generate_job_details(self, name, tasks):
        """Job definition for Asset Bundles"""
        job_clusters = []
        for cluster in self.target_resources["job_clusters"]:
            if next((True for t in tasks if t["job_cluster_key"] == cluster["job_cluster_key"]), False):
                job_clusters.append(cluster)

        job_details = {"name": name, "job_clusters": job_clusters, "tasks": tasks, "tags": self.tags}
        if "email_notifications" in self.target_resources and self.is_notification_enabled:
            job_details["email_notifications"] = self.target_resources["email_notifications"]
        if "tags" in self.target_resources:
            job_details["tags"] = {**self.target_resources["tags"], **self.tags}
        if self.schedule is not None:
            job_details["schedule"] = {"quartz_cron_expression": self.schedule, "timezone_id": self.timezone}
        return job_details

    def generate_workflow(self, name, job_details):
        """Creates a job resource config under resources/*.yml"""
        resources = {"resources": {"jobs": {name: job_details}}}
        with open(Path(f"resources/{name}.yml"), "w") as f:
            yaml.dump(resources, f)

        logging.info(f"Successfully generated '{name}' job.")

    def generate(self):
        """Main method of the class to generate a Databricks workflow yaml file"""
        if self.job_name is None:
            raise ValueError("'job_name' has to be defined to generate a single workflow.")

        if len(self.tasks) > self.max_tasks_per_workflow and self.dbt_cmd != "test":
            raise ValueError(
                f"{len(self.tasks)} tasks generated for a workflow. Maximum: {self.max_tasks_per_workflow}."
            )

        # Databricks has a limit of 500 tasks per workflow
        for i in range((len(self.tasks) // self.max_tests_per_workflow) + 1):
            new_job_name = self.job_name if len(self.tasks) <= self.max_tests_per_workflow else f"{self.job_name}_{i+1}"
            new_tasks = self.tasks[i * self.max_tests_per_workflow : (i + 1) * self.max_tests_per_workflow]
            job_details = self.generate_job_details(new_job_name, new_tasks)
            self.generate_workflow(new_job_name, job_details)


def compile_job(
    dbt_dir,
    target_resources,
    job_name,
    cluster_key,
    schedule,
    version,
    dbt_project,
    dbt_target,
    dbt_command,
    dbt_flags,
    dbt_select,
    dbt_selector,
    dbt_exclude,
    dbt_vars="{}",
    dbt_args="{}",
    task_retries=0,
    task_wait_interval=0,
    task_wait_timeout=0,
    threads=0,
    tags={},
    timezone="UTC",
    is_first_run=True,
    is_redshift_sync=False,
    is_notification_enabled=True,
):
    """Generates a workflow for the specified job"""
    logging.info(f"Starting to generate '{job_name}' job.")
    cluster_key = cluster_key or target_resources["dw_cluster_key"]
    app_cluster_key = target_resources["app_cluster_key"]
    glue_db_name = target_resources["glue_db"]
    default_wait_interval = target_resources.get("wait_interval", 0)
    default_wait_timeout = target_resources.get("wait_timeout", 0)
    default_threads = target_resources.get("threads", 0)
    if dbt_command is None:
        raise ValueError("Missing required 'dbt_command' argument.")
    
    # print("AAA")
    # exit(0)

    os.environ["DBT_ENV_CUSTOM_ENV_JOB"] = job_name # set notebook directory
    if dbt_command == "run-operation":
        os.environ["DBT_ENV_CUSTOM_ENV_MACRO"] = dbt_select # set notebook name

    dbt_task_generator = dbt_commands[dbt_command]
    dbt_tasks = dbt_task_generator(
        dbt_dir=dbt_dir,
        job_name=job_name,
        job_cluster_key=cluster_key,
        dbt_project=dbt_project,
        dbt_target=dbt_target,
        dbt_command=dbt_command,
        dbt_flags=dbt_flags,
        dbt_select=dbt_select,
        dbt_selector=dbt_selector,
        dbt_exclude=dbt_exclude,
        dbt_vars=dbt_vars,
        dbt_args=dbt_args,
        task_retries=task_retries,
        task_wait_interval=task_wait_interval or default_wait_interval,
        task_wait_timeout=task_wait_timeout or default_wait_timeout,
        threads=threads or default_threads,
    )

    # print("!!!!", dbt_tasks, schedule)
    # exit(0)

    if is_first_run:
        dbt_tasks.dbt_setup()

    if version == 3:
        # Tubibricks V3
        manifest_data = dbt_tasks.dry_run()
    elif version == 2:
        # Tubibricks V2
        manifest_data = dbt_tasks.dbt_compile()
        dbt_tasks.compile()
    else:
        raise ValueError(version_error(version))


    databricks_tasks = dbt_tasks.generate()
    if is_redshift_sync and dbt_command in ["model", "run"]:
        application_tasks = dbt_tasks.sync_tasks(glue_db_name, app_cluster_key)
        databricks_tasks.extend(application_tasks)



    job = DatabricksJob(
        job_name=job_name,
        dbt_dir=dbt_dir,
        dbt_command=dbt_command,
        manifest_data=manifest_data,
        target_resources=target_resources,
        tasks=databricks_tasks,
        schedule=schedule,
        tags=tags,
        timezone=timezone,
        is_notification_enabled=is_notification_enabled
    )
    job.generate()


def compile_all_jobs(dbt_dir, target_resources, dbt_project, dbt_target, version):
    """Generates workflows for all tasks defined in the job definition"""
    jobs = target_resources["jobs"]
    from pprint import pprint
    print(list(jobs.keys()))
    for i, key in enumerate(jobs):
        if key == "hourly_time_sensitive":
            print(jobs[key])
        else:
            continue
        job = jobs[key]
        if job.get("enabled", True):
            schedule = job.get("schedule", {})
            compile_job(
                dbt_dir,
                target_resources,
                job_name=job["name"],
                cluster_key=job.get("cluster_key"),
                schedule=schedule.get("quartz_cron_expression"),
                version=version,
                dbt_project=dbt_project,
                dbt_target=dbt_target,
                dbt_command=job["dbt_cmd"],
                dbt_flags=job.get("flags", []),
                dbt_select=job.get("select", ""),
                dbt_selector=job.get("selector", ""),
                dbt_exclude=job.get("exclude", ""),
                dbt_vars=job.get("vars", "{}"),
                dbt_args=job.get("args", "{}"),
                task_retries=job.get("max_retries", 0),
                task_wait_interval=job.get("wait_interval", 0),
                task_wait_timeout=job.get("wait_timeout", 0),
                threads=job.get("threads", 0),
                tags=job.get("tags", {}),
                timezone=schedule.get("timezone_id", "UTC"),
                is_first_run=i == 0,
                is_redshift_sync=True,
                is_notification_enabled=job.get("is_notification_enabled", True)
            )


def compile_range_job(
    dbt_dir,
    target_resources,
    job_name,
    cluster_key,
    version,
    date_ranges,
    dbt_project,
    dbt_target,
    dbt_select,
    dbt_selector,
    dbt_exclude,
    dbt_vars="{}",
    dbt_args="{}",
    task_retries=0,
    task_wait_interval=0,
    task_wait_timeout=0,
    threads=0,
    tags={},
    is_notification_enabled=True,
):
    """Generates a workflow for a backfill with date range and interval"""
    logging.info(f"Starting to generate '{job_name}' for {len(date_ranges)} date ranges.")
    os.environ["DBT_ENV_CUSTOM_ENV_JOB"] = job_name
    job_cluster_key = cluster_key or target_resources["dw_cluster_key"]
    parsed_vars = yaml.safe_load(dbt_vars) or json.loads(dbt_vars)
    manifest_data = None
    databricks_tasks = []
    run_after = []

    for i, date_range in enumerate(date_ranges):
        start_date, end_date = date_range
        range_vars = {**parsed_vars, "start_date": start_date, "end_date": end_date}
        logging.info(f'Generating "{start_date}" - "{end_date}" date range.')
        dbt_task = DbtModelTask(
            dbt_dir=dbt_dir,
            job_name=job_name,
            job_cluster_key=job_cluster_key,
            dbt_project=dbt_project,
            dbt_target=dbt_target,
            dbt_command="model",
            dbt_flags=[],
            dbt_select=dbt_select,
            dbt_selector=dbt_selector,
            dbt_exclude=dbt_exclude,
            dbt_vars=json.dumps(range_vars),
            dbt_args=dbt_args,
            task_retries=task_retries,
            task_wait_interval=task_wait_interval,
            task_wait_timeout=task_wait_timeout,
            threads=threads,
        )

        if i == 0:
            dbt_task.dbt_setup()

        if version == 3:
            # Tubibricks V3
            os.environ["DBT_ENV_CUSTOM_ENV_NOTEBOOK_SUFFIX"] = start_date
            manifest_data = dbt_task.dry_run()
        elif version == 2:
            # Tubibricks V2
            manifest_data = dbt_task.dbt_compile()
            dbt_task.compile(task_suffix=start_date)
        else:
            raise ValueError(version_error(version))

        tasks = dbt_task.generate(task_suffix=start_date, run_after=run_after)
        databricks_tasks += tasks
        run_after = [task["task_key"] for task in tasks]

    job = DatabricksJob(
        job_name=job_name,
        dbt_dir=dbt_dir,
        dbt_command="model",
        manifest_data=manifest_data,
        target_resources=target_resources,
        tasks=databricks_tasks,
        schedule=None,
        tags=tags,
        is_notification_enabled=is_notification_enabled
    )
    job.generate()


def run(repo_dir):
    job_name = args.job_name
    schedule = args.schedule
    cluster_key = args.cluster_key
    date_ranges = parse_range_args(args.start, args.end, {"days": args.days, "weeks": args.weeks, "months": args.months})
    # branch = args.branch
    task_retries = args.retries
    task_wait_interval = args.wait_interval
    task_wait_timeout = args.wait_timeout
    threads = args.threads
    job_tags = args.tags
    is_redshift_sync = args.sync_redshift
    is_notification_enabled = args.enabled_notifications
    version = args.version
    dbt_project = args.project
    dbt_target = args.target
    dbt_dir = f"{repo_dir}/{dbt_project}" if repo_dir else dbt_project
    dbt_flags = ['--full-refresh'] if args.full_refresh else []
    dbt_command = args.command
    dbt_select = args.select
    dbt_selector = args.selector
    dbt_exclude = args.exclude
    dbt_vars = args.vars
    dbt_args = args.args


    if dbt_project is None:
        raise ValueError("Missing required 'project' argument.")

    if dbt_target is None:
        raise ValueError("Missing required 'target' argument.")
    


    job_definition_file = f"{dbt_project}.yml"
    print("!!",job_definition_file)
    job_definition = load_job_definition(job_definition_file, dbt_dir)
    validate_dbt_job_definition(job_definition)
    resources = job_definition["resources"]
    if dbt_target not in resources:
        raise ValueError(f"Cannot find dbt_target '{dbt_target}' in {job_definition_file}.")

    target_resources = resources[dbt_target]

    os.environ["DBT_ENV_CUSTOM_ENV_DRY"] = "notebook"
    tags = yaml.safe_load(job_tags)

    print("BBBB", job_name, date_ranges)

    if job_name is None:
        compile_all_jobs(dbt_dir, target_resources, dbt_project, dbt_target, version)
    elif len(date_ranges) > 0:
        compile_range_job(
            dbt_dir,
            target_resources,
            job_name,
            cluster_key,
            version,
            date_ranges,
            dbt_project,
            dbt_target,
            dbt_select,
            dbt_selector,
            dbt_exclude,
            dbt_vars,
            dbt_args,
            task_retries,
            task_wait_interval,
            task_wait_timeout,
            threads,
            tags,
            is_notification_enabled=is_notification_enabled,
        )
    else:
        compile_job(
            dbt_dir,
            target_resources,
            job_name,
            cluster_key,
            schedule,
            version,
            dbt_project,
            dbt_target,
            dbt_command,
            dbt_flags,
            dbt_select,
            dbt_selector,
            dbt_exclude,
            dbt_vars,
            dbt_args,
            task_retries,
            task_wait_interval,
            task_wait_timeout,
            threads,
            tags,
            is_redshift_sync=is_redshift_sync,
            is_notification_enabled=is_notification_enabled,
        )

    logging.info("Done.")


# MAIN
if __name__ == "__main__":
    REPO_DIR = os.getenv("GITHUB_WORKSPACE")
    run(REPO_DIR)
