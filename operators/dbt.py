import os

from typing import Any, Union, List

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.decorators import apply_defaults

from hooks.dbt import DbtCliHook


class DbtBaseOperator(BaseOperator):
    """
    Base dbt operator
    All other dbt operators are derived from this operator.

    :param env: If set, passes the env variables to
        the subprocess handler
    :type env: dict
    :param profiles_dir: If set, passed as
        the `--profiles-dir` argument to the `dbt` command
    :type profiles_dir: str
    :param target: If set, passed as the `--target` argument
        to the `dbt` command
    :type dir: str
    :param dir: The directory to run the CLI in
    :type vars: str
    :param vars: If set, passed as the `--vars` argument
        to the `dbt` command
    :type vars: dict
    :param full_refresh: If `True`, will fully-refresh incremental models.
    :type full_refresh: bool
    :param models: If set, passed as the `--models` argument
        to the `dbt` command
    :type models: str
    :param warn_error: If `True`, treat warnings as errors.
    :type warn_error: bool
    :param exclude: If set, passed as the `--exclude` argument
        to the `dbt` command
    :type exclude: str
    :param select: If set, passed as the `--select` argument
        to the `dbt` command
    :type select: str
    :param selector: If set, passed as the `--selector` argument
        to the `dbt` command
    :type selector: str
    :param dbt_bin: The `dbt` CLI. Defaults to `dbt`, so assumes
        it's on your `PATH`
    :type dbt_bin: str
    :param verbose: The operator will log verbosely to
        the Airflow logs
    :type verbose: bool
    """

    ui_color = "#d6522a"

    template_fields = (
        "env",
        "vars",
        "warehouse_name",
        "warehouse_size",
    )
    template_fields_renderers = {
        "env": "json",
        "vars": "json",
        "warehouse_name": "html",
        "warehouse_size": "html",
    }

    @apply_defaults
    def __init__(
        self,
        *args,
        env=None,
        profiles_dir=None,
        target=None,
        dir=".",
        vars=None,
        models=None,
        exclude=None,
        select=None,
        selector=None,
        dbt_bin="dbt",
        verbose=True,
        warn_error=False,
        full_refresh=False,
        data=False,
        schema=False,
        warehouse_name=None,
        warehouse_size=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.env = env or {}
        self.profiles_dir = profiles_dir
        self.target = target
        self.dir = dir
        self.vars = vars
        self.models = models
        self.full_refresh = full_refresh
        self.data = data
        self.schema = schema
        self.exclude = exclude
        self.select = select
        self.selector = selector
        self.dbt_bin = dbt_bin
        self.verbose = verbose
        self.warn_error = warn_error
        self.warehouse_name = warehouse_name
        self.warehouse_size = warehouse_size
        self.create_hook()

    def create_hook(self):
        self.hook = DbtCliHook(
            env=self.env,
            profiles_dir=self.profiles_dir,
            target=self.target,
            dir=self.dir,
            vars=self.vars,
            full_refresh=self.full_refresh,
            data=self.data,
            schema=self.schema,
            models=self.models,
            exclude=self.exclude,
            select=self.select,
            selector=self.selector,
            dbt_bin=self.dbt_bin,
            verbose=self.verbose,
            warn_error=self.warn_error,
        )

        return self.hook

    def handle_dbt_error(self, ae) -> AirflowException:
        """
        Handle dbt errors, retrying with full_refresh = True
        if the error is caused by a schema change on an incremental model

        :param ae: AirflowException
        :return: AirflowException
        """
        essential_models = Variable.get(
            "dbt_essential_models", deserialize_json=True, default_var=[]
        )
        if "out of sync" in str(ae) and not self.full_refresh:
            if self.models not in essential_models:
                self.log.info(
                    """
                    Error has been caused by schema change on
                    incremental model, retrying with full_refresh = True
                """
                )
                self.full_refresh = True
                self.create_hook().run_cli("run")

                return None
            else:
                self.log.info(
                    """
                    Not retrying with full_refresh = True because
                    the model is considered an essential model
                    Please check the source table and trigger a full-refresh
                    manually if required
                """
                )
                raise ae
        else:
            raise ae

    def set_warehouse_size(
        self, context, name, size
    ) -> Union[Any, List[Any], None]:
        """
        Set the warehouse size for execution
        when triggering a manual full-refresh

        :param context: Airflow context
        :param name: Warehouse name
        :param size: Warehouse size
        :return: (Any | list[Any] | None)
        """
        return SnowflakeHook(snowflake_conn_id=os.environ['SNOWFLAKE_CONN_ID']).run(
            f"ALTER WAREHOUSE {name.upper()} SET WAREHOUSE_SIZE = {size.upper()};"
        )

    def set_warehouse_name(self, context, name) -> None:
        """
        Set the warehouse for execution when triggering a manual full-refresh

        :param context: Airflow context
        :param name: Warehouse name
        :return: None
        """
        self.env["DBT_WAREHOUSE"] = name
        return

    def set_warehouse(self, context, name, size) -> Exception:
        """
        Set the warehouse for execution when triggering a manual full-refresh

        :param context: Airflow context
        :param name: Warehouse name
        :param size: Warehouse size
        :return: ValueError
        """
        try:
            self.set_warehouse_name(context, name)
            self.set_warehouse_size(context, name, size)
        except Exception as e:
            self.log.error(f"exception caught setting warehouse size: {e}")
            raise e


class DbtRunOperator(DbtBaseOperator):
    """
    Main airflow operator to run dbt with our
    custom logic for incremental tables that
    need full-refresh
    """

    @apply_defaults
    def __init__(self, *args, profiles_dir=None, target=None, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir, target=target, *args, **kwargs
        )

    def execute(self, context):
        full_refresh = context["dag_run"].conf.get("full_refresh", False)
        self.full_refresh = full_refresh
        # If the DAG has been triggered with a configuration like:
        # {
        #   "full_refresh": true,
        #   "models": ["model_name"],
        #   "warehouse_name": "warehouse_name",
        #   "warehouse_size": "warehouse_size"
        # }
        # Then trigger only the models in that dictionary,
        # and with a --full-refresh flag, setting the warehouse
        # for execution to the sepcified warehouse at the indicated size.
        # If not, just trigger them as usual (full-refresh = False)
        if "models" in context["dag_run"].conf and self.models not in context[
            "dag_run"
        ].conf.get("models", []):
            self.log.info(f"Skipping execution for model {self.models}...")
            return
        # If the DAG has been triggered with a configuration
        # check if warehouse name and size is specified,
        # if not don't execute and log it.
        # This enforces the need to be explicit about which warehouse to use and at which size.
        try:
            if (
                "warehouse_name" in context["dag_run"].conf
                and "warehouse_size" in context["dag_run"].conf
            ):
                self.set_warehouse(
                    context,
                    context["dag_run"].conf.get("warehouse_name", None),
                    context["dag_run"].conf.get("warehouse_size", None),
                )
        except Exception as e:
            self.log.error(f"Unhandled exception: {e}")
            raise e

        try:
            self.create_hook().run_cli("run")
        except AirflowException as ae:
            # If we get a `dbt run` 'out of sync' error (incremental model),
            # then run the model again with --full-refresh flag automatically,
            # excepting for "essential models"
            try:
                self.handle_dbt_error(ae)
            except Exception as e:
                self.log.error(f"Unhandled exception: {e}")
                raise e


class DbtTestOperator(DbtBaseOperator):
    @apply_defaults
    def __init__(self, *args, profiles_dir=None, target=None, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir, target=target, *args, **kwargs
        )

    def execute(self, context):
        self.create_hook().run_cli("test")


class DbtDocsGenerateOperator(DbtBaseOperator):
    @apply_defaults
    def __init__(self, *args, profiles_dir=None, target=None, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir, target=target, *args, **kwargs
        )

    def execute(self, context):
        self.create_hook().run_cli("docs", "generate")


class DbtSnapshotOperator(DbtBaseOperator):
    @apply_defaults
    def __init__(self, *args, profiles_dir=None, target=None, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir, target=target, *args, **kwargs
        )

    def execute(self, context):
        self.create_hook().run_cli("snapshot")


class DbtSeedOperator(DbtBaseOperator):
    @apply_defaults
    def __init__(self, *args, profiles_dir=None, target=None, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir, target=target, *args, **kwargs
        )

    def execute(self, context):
        self.create_hook().run_cli("seed")


class DbtDepsOperator(DbtBaseOperator):
    @apply_defaults
    def __init__(self, *args, profiles_dir=None, target=None, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir, target=target, *args, **kwargs
        )

    def execute(self, context):
        self.create_hook().run_cli("deps")


class DbtCleanOperator(DbtBaseOperator):
    @apply_defaults
    def __init__(self, *args, profiles_dir=None, target=None, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir, target=target, *args, **kwargs
        )

    def execute(self, context):
        self.create_hook().run_cli("clean")
