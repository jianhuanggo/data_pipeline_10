from inspect import currentframe
import argparse
from logging import Logger as Log
from _common import _common as _common_
from _config import config as _config_

__version__ = 0.2


class AwsApiCLI:

    def __init__(self, config: _config_.AwsApiConfig):
        self._config = config

    def get_parser(self, logger: Log = None):
        """

        Args:
            logger: Whether error msg should be persisted in a log file

        Returns:

        """

        try:
            parser = argparse.ArgumentParser(description="This is data pipeline that takes json array as input "
                                                         "and transform data into wavefront alerts in yaml which then"
                                                         "can be consumed by terraform")
            parser.add_argument("-v", "--version", action="version", version="%(prog)s VERSION " + str(__version__),
                                help="show current version")
            parser.add_argument("-b", "--build_type", choices=["release", "pr"], dest="identity_build_type",
                                required=True, help="build type")
            parser.add_argument("-p", "--pull_request", action="store", type=str, dest="identity_pull_request",
                                required=True, help="pull request")
            parser.add_argument("-t", "--git_token", action="store", type=str, dest="identity_git_token",
                                required=True, help="encrypted git token")
            parser.add_argument("-st", "--slack_token", action="store", type=str, dest="identity_slack_token",
                                required=True, help="encrypted slack token")
            parser.add_argument("-pt", "--pagerduty_token", action="store", type=str, dest="identity_pagerduty_token",
                                required=True, help="encrypted pagerduty token")
            parser.add_argument("-w", "--working_dir", action="store", type=str, dest="identity_working_dir",
                                default=self._config.config.get("working_directory"), help="working directory")
            parser.add_argument("-a", "--aws_access_key_id", action="store", type=str, dest="identity_aws_access_key_id",
                                help="_aws access key id")
            parser.add_argument("-s", "--aws_secret_access_key", action="store", type=str, dest="identity_aws_secret_access_key",
                                help="_aws secret access key")
            parser.add_argument("-ve", "--verification_environment", choices=["e2e", "prd"], type=str, dest="identity_verification_environment",
                                help="environment, use for alert object verification")
            parser.add_argument("-d", "--dry_run", action="store", type=bool, dest="identity_dry_run", default=False,
                                help="dry run, will only output to files")
            parser.add_argument("-vy", "--verify", action="store", type=bool, dest="identity_verify", default=False,
                                help="only verify alert object counts between yaml input files and terraform state.")
            parser.add_argument("-rb", "--rollback", action="store", type=bool, dest="identity_rollback", default=False,
                                help="only rollback to the previous version")

            args = parser.parse_args()
            print(f"Here are the arguments passed in:")
            print(f"build type: {args.identity_build_type}")
            print(f"pull request: {args.identity_pull_request}")
            print(f"git token specified: {args.identity_git_token is not None}")
            print(f"working directory: {args.identity_working_dir}")
            print(f"dry run: {args.identity_dry_run}")
            return args
        except Exception as err:
            _common_.error_logger(currentframe().f_code.co_name,
                                  err,
                                  logger=logger,
                                  mode="error",
                                  ignore_flag=False)


def get_parser(config: _config_.AwsApiConfig):
    """

    Args:
        config: default configuration file

    Returns:

    """
    return AwsApiCLI(config).get_parser()
