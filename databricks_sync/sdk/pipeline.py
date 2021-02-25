import abc
import asyncio
import copy
import fnmatch
import json
import traceback
from abc import ABC
from functools import reduce, singledispatch
from pathlib import Path
from typing import List, Callable, Generator, Any, Dict, Union, Tuple

from databricks_cli.sdk import ApiClient
from streamz import Stream
from tenacity import wait_fixed, retry

from databricks_sync import log
from databricks_sync.sdk.hcl.json_to_hcl import TerraformJsonBuilder
from databricks_sync.sdk.message import HCLConvertData, APIData, Artifact
from databricks_sync.sdk.processor import Processor, MappedGrokVariableBasicAnnotationProcessor
from databricks_sync.sdk.report.model import event_manager, EventManager, run_id, Session
from databricks_sync.sdk.utils import normalize


class APIGenerator(abc.ABC):

    def __init__(self, api_client: ApiClient, base_path: Path,
                 patterns=None,
                 ):
        self._patterns = patterns or []
        self._base_path = base_path
        self.__api_client = api_client
        self._is_dask_enabled = False
        self._buffer = 8
        self.source = Stream(stream_name=self.folder_name)

    def set_dask_conf(self, is_dask_enabled=True, buffer=8):
        self._is_dask_enabled = is_dask_enabled
        self._buffer = buffer

    def _match_patterns(self, key):
        # TODO: determine if this should be any or all (and clause/or clause)
        matched = all([fnmatch.fnmatch(key, pattern) for pattern in self._patterns])
        log.debug(f"Attempt to match {key} to patterns: {self._patterns} yielded in {matched}")
        return matched

    @property
    @abc.abstractmethod
    def folder_name(self) -> str:
        pass

    @staticmethod
    def __add_tf_suffix(file_name: str) -> str:
        return file_name + ".tf.json"

    def get_relative_hcl_path(self, file_name) -> str:
        return str(ExportFileUtils.make_relative_path(
            self.folder_name,
            self.__add_tf_suffix(file_name)
        ))

    def get_local_hcl_path(self, file_name) -> Path:
        return ExportFileUtils.make_local_path(
            self._base_path,
            self.folder_name,
            self.__add_tf_suffix(file_name)
        )

    @property
    def resource_folder_name(self):
        return self.folder_name.replace("databricks_", "")

    @property
    def api_client(self):
        return self.__api_client

    def map_processors(self, map_var_dot_path_dict) -> List[Processor]:
        processors = []
        if map_var_dot_path_dict is not None and len(list(map_var_dot_path_dict.keys())) > 0:
            processors.append(MappedGrokVariableBasicAnnotationProcessor(self.folder_name,
                                                                         dot_path_grok_dict=map_var_dot_path_dict))
        return processors

    def create_stream(self):
        return self.source

    async def trigger(self):
        async for item in self.generate():
            self.source.emit(item)

    async def generate(self):
        async for item in self._generate():
            item: HCLConvertData
            log.info(f"Processing: {item.resource_name} with name: {item.human_readable_name} and id: {item.raw_id}")
            if len(item.for_each_var_id_name_pairs) > 0:
                for id_name_pairs in item.for_each_var_id_name_pairs:
                    event_manager.make_start_record(item.workspace_url, id_name_pairs[0],
                                                    item.resource_name, id_name_pairs[0],
                                                    id_name_pairs[1])
            else:
                event_manager.make_start_record(item.workspace_url, item.hcl_resource_identifier, item.resource_name,
                                                item.raw_id, item.human_readable_name)
            yield item

    @abc.abstractmethod
    async def _generate(self) -> Generator[APIData, None, None]:
        pass

    def _create_data(self,
                     resource_type: str,
                     data: Dict[str, Any],
                     filter_func: Callable[[], bool],
                     identifier_func: Callable[[Dict[str, Any]], str],
                     raw_id_func: Callable[[Dict[str, Any]], str],
                     make_dict_func: Callable[[Dict[str, Any]], Dict[str, Any]],
                     processors,
                     human_readable_name_func: Callable[[Dict[str, Any]], str] = None,
                     ):
        if filter_func():
            return None
        identifier = identifier_func(data)  # normalizes the identifier
        api_data = APIData(
            self.get_raw_id(data, raw_id_func),
            self.api_client.url,
            identifier,
            make_dict_func(data),
            self.get_local_hcl_path(identifier),
            relative_save_path=self.get_relative_hcl_path(identifier),
            human_readable_name=human_readable_name_func(data) if human_readable_name_func is not None else None
        )
        processed_api_data = self.post_process_api_data_hook(data, api_data)
        return HCLConvertData(resource_type, processed_api_data,
                              processors=processors)

    @normalize
    def get_identifier(self, data: Dict[str, Any], data_func: Callable[[Dict[str, Any]], str]) -> str:
        return data_func(data)

    def get_raw_id(self, data: Dict[str, Any], data_func: Callable[[Dict[str, Any]], str]) -> str:
        return data_func(data)

    def get_pattern_dot_paths(self, data: Dict[str, Any]) -> List[str]:
        # Default is raw_id, else it should be overridden
        return [self.get_raw_id(data)]

    def post_process_api_data_hook(self, data: Dict[str, Any], api_data: APIData) -> APIData:
        return api_data

class StreamUtils:

    @staticmethod
    def merge_sources(sources: List[Stream], is_dask_enabled: bool = True):
        if sources is None or len(sources) == 0:
            raise ValueError("unable to merge sources as sources list is empty")
        if is_dask_enabled:
            if len(sources) == 1:
                return sources[0]
            return sources[0].scatter().union(*[s.scatter() for s in sources[1:]]).gather()
        else:
            all_streams = [src for src in sources]
            return reduce(lambda x, y: x.union(y), all_streams)

    @staticmethod
    def __verify_error(func: Callable[[HCLConvertData], Any]):
        if not hasattr(func, "managed_error"):
            raise ValueError(f"function {func.__name__} does not have its error managed. Please use " +
                             "BaseTerraformModel.manage_error decorator to make sure that error is propagated "
                             "through the pipeline.")

    @staticmethod
    def apply_map(func: Callable[[HCLConvertData], HCLConvertData], stream: Stream,
                  is_dask_enabled: bool = True, buffer: int = 8):
        StreamUtils.__verify_error(func)
        # map_func = functools.partial(StreamUtils.__map_pass_error, func=func)
        if is_dask_enabled:
            return stream.scatter().map(func).buffer(buffer).gather()
        else:
            return stream.map(func)

    @staticmethod
    def apply_filter(func: Callable[[HCLConvertData], bool], stream: Stream):
        StreamUtils.__verify_error(func)
        return stream.filter(func)


@singledispatch
def write_file(data, path: Path):
    raise ValueError(f"Data should be either a str or bytes like but got: {type(data)}")


@write_file.register(str)
def _(data, path: Path):
    with path.open("w+") as f:
        f.write(data)


@write_file.register(bytes)
def _(data, path: Path):
    with path.open("wb+") as f:
        f.write(data)


class ExportFileUtils:
    BASE_DIRECTORY = "exports"

    @staticmethod
    def __ensure_parent_dirs(dir_path):
        Path(dir_path).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def make_mapped_vars_path(base_path: str) -> Path:
        dir_path = Path(base_path) / ExportFileUtils.BASE_DIRECTORY
        ExportFileUtils.__ensure_parent_dirs(dir_path)
        return dir_path / "mapped_variables.tf.json"

    @staticmethod
    def make_tfvars(base_path: str) -> Path:
        dir_path = Path(base_path)
        ExportFileUtils.__ensure_parent_dirs(dir_path)
        return dir_path / "terraform.tfvars"

    @staticmethod
    def make_tfvars_env_file(base_path: str) -> Path:
        dir_path = Path(base_path)
        ExportFileUtils.__ensure_parent_dirs(dir_path)
        return dir_path / "variables_env.sh"

    @staticmethod
    def make_local_data_path(base_path: Path, sub_dir: str, file_name) -> Path:
        dir_path = base_path / ExportFileUtils.BASE_DIRECTORY / sub_dir / "data"
        ExportFileUtils.__ensure_parent_dirs(dir_path)
        return dir_path / file_name

    @staticmethod
    def make_local_path(base_path: Path, sub_dir: str, file_name) -> Path:
        dir_path = Path(base_path) / ExportFileUtils.BASE_DIRECTORY / sub_dir
        ExportFileUtils.__ensure_parent_dirs(dir_path)
        return dir_path / file_name

    @staticmethod
    def make_relative_path(sub_dir: str, file_name) -> Path:
        dir_path = Path(ExportFileUtils.BASE_DIRECTORY) / sub_dir
        return dir_path / file_name

    @staticmethod
    def add_file(local_path: Path, data: Union[str, bytes]):
        log.info(f"Writing to path {str(local_path)}")
        write_file(data, path=local_path)


class DownloaderAPIGenerator(APIGenerator, ABC):

    @staticmethod
    @HCLConvertData.manage_error
    def _download(hcl_convert_data: HCLConvertData) -> HCLConvertData:
        for artifact in hcl_convert_data.artifacts:
            content = artifact.get_content()
            log.info("Content fetched :-) for " + artifact.remote_path + " with length " +
                     str(len(content)))
            ExportFileUtils.add_file(artifact.local_path, content)
        return hcl_convert_data

    def get_local_download_path(self, file_name):
        return ExportFileUtils.make_local_data_path(
            self._base_path,
            self.resource_folder_name,
            file_name
        )

    def _create_stream(self):
        return StreamUtils.apply_map(self._download, super().create_stream(), self._is_dask_enabled, self._buffer)

    def create_stream(self):
        return self._create_stream()

    @abc.abstractmethod
    def construct_artifacts(self, data: Dict[str, Any]) -> List[Artifact]:
        pass

    def post_process_api_data_hook(self, data: Dict[str, Any], api_data: APIData) -> APIData:
        artifacts = self.construct_artifacts(data)
        new_api_data = api_data.clone_with(artifacts=artifacts)
        return new_api_data


def before_retry(fn, attempt_number):
    log.debug(f"Attempt {attempt_number}: attempting to retry {fn.__name__}")


class PipelineResults:

    def __init__(self):
        self.summary = {}
        try:
            self._session = Session()
            self._event_manager = EventManager(run_id=run_id, session=self._session)
        except Exception:
            log.warn("Failed to initialize a session for SQLite to store results")

    def __str__(self):
        return json.dumps(self.summary, indent=4)

    def __handle_failed_events(self, hcl_convert_data: HCLConvertData):
        error_list = []
        for err in hcl_convert_data.errors:
            error_list.append("\n".join(traceback.format_exception(type(err), err, err.__traceback__)))
        if len(hcl_convert_data.for_each_var_id_name_pairs) > 0:
            for id_name_pair in hcl_convert_data.for_each_var_id_name_pairs:
                self._event_manager.make_end_record(hcl_convert_data.workspace_url,
                                                    id_name_pair[0],
                                                    hcl_convert_data.resource_name,
                                                    "FAILED",
                                                    error_msg="\n".join([str(err) for err in hcl_convert_data.errors]),
                                                    error_traceback="\n".join(error_list)
                                                    )
        else:
            self._event_manager.make_end_record(hcl_convert_data.workspace_url,
                                                hcl_convert_data.hcl_resource_identifier,
                                                hcl_convert_data.resource_name,
                                                "FAILED",
                                                error_msg="\n".join([str(err) for err in hcl_convert_data.errors]),
                                                error_traceback="\n".join(error_list),
                                                )

    def __handle_passed_events(self, hcl_convert_data: HCLConvertData):
        if len(hcl_convert_data.for_each_var_id_name_pairs) > 0:
            for id_name_pair in hcl_convert_data.for_each_var_id_name_pairs:
                self._event_manager.make_end_record(hcl_convert_data.workspace_url,
                                                    id_name_pair[0],
                                                    hcl_convert_data.resource_name,
                                                    "SUCCEDED", file_path=hcl_convert_data.relative_save_path)
        else:
            self._event_manager.make_end_record(hcl_convert_data.workspace_url,
                                                hcl_convert_data.hcl_resource_identifier,
                                                hcl_convert_data.resource_name,
                                                "SUCCEDED", file_path=hcl_convert_data.relative_save_path)

    def log_events(self, hcl_convert_data: HCLConvertData):
        if len(hcl_convert_data.errors) > 0:
            self.__handle_failed_events(hcl_convert_data)
        else:
            self.__handle_passed_events(hcl_convert_data)

    def add_hcl_data(self, hcl_convert_data: HCLConvertData):
        self.log_events(hcl_convert_data)


class Pipeline:

    def __init__(self, generators: List[APIGenerator], base_path: str, sinks=None,
                 dask_client=None, debug_mode=False, ):
        self._base_path = base_path
        self.__dask_client = dask_client
        self.__debug_mode = debug_mode
        self.__sinks = sinks
        self.__collectors = []
        self.__generators = generators
        self.__pipeline_results = PipelineResults()

    @property
    def has_dask_client(self):
        return self.__dask_client is not None

    @staticmethod
    @HCLConvertData.manage_error
    def apply_processors(terraform_model: HCLConvertData):
        tf_model = copy.deepcopy(terraform_model)
        if terraform_model.processors is None:
            return tf_model
        for processor in terraform_model.processors:
            processor.process(tf_model)
        return tf_model

    @staticmethod
    def make_resource_files_handler(debug: bool):
        @HCLConvertData.manage_error
        def _save_hcl(hcl_convert_data: HCLConvertData):
            ExportFileUtils.add_file(hcl_convert_data.local_save_path, hcl_convert_data.to_hcl(debug))
            return hcl_convert_data

        return _save_hcl

    @staticmethod
    @HCLConvertData.manage_error
    def mapped_variables_unique_key(hcl_convert_data: HCLConvertData) -> str:
        tjb = TerraformJsonBuilder()
        # TODO: fix this we should also throw a duplicate variable error rather than value error as well
        for mapped_var in hcl_convert_data.mapped_variables:
            try:
                tjb.add_variable(mapped_var.variable_name, mapped_var.to_dict())
            except ValueError as e:
                log.debug(f"Attempting to find unique but found duplicate of {mapped_var} so skipping.")
        return tjb.to_json()
        # return "\n".join([mapped_var.to_hcl(False) for mapped_var in hcl_convert_data.mapped_variables])

    @staticmethod
    @HCLConvertData.manage_error
    def filter_mapped_variables(hcl_convert_data: HCLConvertData) -> bool:
        if hcl_convert_data.mapped_variables is not None \
                and len(hcl_convert_data.errors) == 0 \
                and len(hcl_convert_data.mapped_variables) > 0:
            return True
        return False

    @staticmethod
    @HCLConvertData.manage_error
    def filter_tfvars(hcl_convert_data: HCLConvertData) -> bool:
        contains_tf_vars = \
            (hcl_convert_data.resource_variables is not None or hcl_convert_data.mapped_variables is not None) \
            and (len(hcl_convert_data.resource_variables) > 0 or len(hcl_convert_data.mapped_variables) > 0) \
            and len(hcl_convert_data.errors) == 0
        log.debug(f"contains tf vars: {contains_tf_vars}")
        if contains_tf_vars:
            return True
        return False

    @staticmethod
    @HCLConvertData.manage_error
    def map_tfvars(hcl_convert_data: HCLConvertData) -> List[Tuple[str, str]]:
        tfvars = []

        for m_var in hcl_convert_data.mapped_variables:
            tfvars.append((m_var.variable_name, m_var.default))
        for r_var in hcl_convert_data.resource_variables:
            tfvars.append((r_var.variable_name, r_var.default))
        log.debug(f"tfvars-{tfvars}")
        return tfvars

    @staticmethod
    def make_mapped_variables_handler(base_path):
        @HCLConvertData.manage_error
        def _save_mapped_variables(hcl_convert_data_list: List[HCLConvertData]):
            tjb = TerraformJsonBuilder()
            for hcl_convert_data in hcl_convert_data_list:
                for mapped_var in hcl_convert_data.mapped_variables:
                    try:
                        tjb.add_variable(mapped_var.variable_name, mapped_var.to_dict())
                    except ValueError as e:
                        log.debug(f"Attempting to add another instance of {mapped_var} so skipping.")
                    # tjb.add_variable(mapped_var.variable_name, mapped_var.to_dict())
            mapped_variables_json = tjb.to_json()
            with ExportFileUtils.make_mapped_vars_path(base_path).open("w+") as f:
                f.write(mapped_variables_json)
                f.flush()
            return hcl_convert_data_list

        return _save_mapped_variables

    @staticmethod
    def make_tfvars_handler(base_path):
        def _save_tfvars(vars: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
            with ExportFileUtils.make_tfvars(base_path).open("w+") as f:
                for var in vars:
                    val = var[1] if var[1] is not None else ''
                    f.write(f'{var[0]}="{val}"\n')
                    f.flush()
            with ExportFileUtils.make_tfvars_env_file(base_path).open("w+") as f:
                for var in vars:
                    val = var[1] if var[1] is not None else ''
                    f.write(f'export TF_VAR_{var[0]}="{val}"\n')
                    f.flush()
            return vars

        return _save_tfvars

    def wire(self):
        debug = False

        for g in self.__generators:
            g.set_dask_conf(self.has_dask_client, buffer=8)

        unioned_stream = StreamUtils.merge_sources([g.create_stream() for g in self.__generators],
                                                   is_dask_enabled=self.has_dask_client)

        processed_stream = StreamUtils.apply_map(Pipeline.apply_processors, unioned_stream,
                                                 is_dask_enabled=self.has_dask_client)
        map_vars_s = StreamUtils.apply_filter(
            Pipeline.filter_mapped_variables,
            processed_stream)
        map_vars_collector = map_vars_s.unique(key=Pipeline.mapped_variables_unique_key).collect()
        self.__collectors.append(map_vars_collector)

        StreamUtils.apply_map(
            Pipeline.make_mapped_variables_handler(self._base_path),
            map_vars_collector,
            # Everything will be collected to all in once place we do not need this to be distributed
            is_dask_enabled=False
        ).sink(lambda x: x)

        tfvars_s = StreamUtils.apply_filter(
            Pipeline.filter_tfvars,
            processed_stream)
        # Filter all errored out data and then process the variables
        tfvars_values_s = tfvars_s.filter(lambda data: len(data.errors) == 0).map(Pipeline.map_tfvars)

        tfvars_collector = tfvars_values_s.flatten().unique().collect()
        self.__collectors.append(tfvars_collector)
        tfvars_collector.map(Pipeline.make_tfvars_handler(self._base_path)).sink(lambda x: x)

        resource_s = StreamUtils.apply_map(
            Pipeline.make_resource_files_handler(debug),
            processed_stream,
            is_dask_enabled=self.has_dask_client
        )
        resource_s.sink(self.__pipeline_results.add_hcl_data)

    @retry(wait=wait_fixed(10), before=before_retry)
    def __wait_for_all_dask_futures(self) -> None:
        snapshot = list(self.__dask_client.futures)  # client modifies future and you cannot iterate on a mutating dict
        for key in snapshot:
            if self.__dask_client.futures[key].status == "pending":
                # TODO: add log statement here
                raise ValueError("expecting all futures to be finished")
        log.info("Waiting for all processes to be finished.")

    def __generate_all(self):
        # finish up initial push of events
        loop = asyncio.get_event_loop()
        groups = asyncio.gather(*[stream.trigger() for stream in self.__generators])
        loop.run_until_complete(groups)
        if self.__dask_client is not None:
            self.__wait_for_all_dask_futures()

    def __flush_map_var_collectors(self):
        for collector in self.__collectors:
            collector.flush()
        if self.__dask_client is not None:
            self.__wait_for_all_dask_futures()

    def run(self):
        self.__generate_all()
        self.__flush_map_var_collectors()
        log.info(self.__pipeline_results)
