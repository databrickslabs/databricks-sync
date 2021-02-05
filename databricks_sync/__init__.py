import logging

logging.basicConfig(
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])
API_VERSION_2_0 = "2.0"
API_VERSION_1_2 = "1.2"

api_client_var_dict = {
    API_VERSION_2_0: "api_client",
    API_VERSION_1_2: "api_client_v1_2"
}
