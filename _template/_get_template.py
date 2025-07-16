from tempfile import template
from typing import List, Set
from typing import Optional, Dict
from _common import _common as _common_
from jinja2 import Template, Environment,meta

@_common_.exception_handler
def render(template_name: str,
           parameters: Dict[str, str]) -> str:
    return Template(template_name).render(parameters)

@_common_.exception_handler
def extract_variables(template: object) -> set[str]:

    from _management._meta import _inspect_module

    env = Environment()
    parsed_content = env.parse(_inspect_module.get_source(template))

    variables = meta.find_undeclared_variables(parsed_content)
    return variables



