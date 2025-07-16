command_10 = """
   mkdir -p {{ DW_HOME }}/dw/tubibricks/src/{{ MODEL_NAME }}/models/{{ MODEL_DIR }}/
"""
# backup directory
command_15 = """
    mkdir -p /Users/jian.huang/projects/backup/{{ PROJECT_ID }}/
"""

# backup
command_16 = """
   cp {{ DW_HOME }}/dw/tubibricks/models/{{ MODEL_DIR }}/{{ MODEL_NAME }}.sql /Users/jian.huang/projects/backup/{{ PROJECT_ID }}/

"""

command_17 = """
   cp {{ DW_HOME }}/dw/tubibricks/src/{{ MODEL_NAME }}/models/{{ MODEL_DIR }}/* /Users/jian.huang/projects/backup/{{ PROJECT_ID }}/
   
"""

command_18 = """
   cp {{ DW_HOME }}/dw/tubibricks/resources/{{ MODEL_NAME }}.yml /Users/jian.huang/projects/backup/{{ PROJECT_ID }}/

"""


command_20 = """
    rm -rf {{ DW_HOME }}/dw/tubibricks/src/{{ MODEL_NAME }}/models/{{ MODEL_DIR }}/

"""

command_30 = """
    cd {{ DW_HOME }}/dw

"""

command_40 = """
    <<< working_dir: {{ DW_HOME }}/dw >>>
    cd {{ DW_HOME }}/dw && git checkout {{ GITHUB_BRANCH }}

"""

command_50 = """
    mkdir -p {{ DW_HOME }}/dw/tubibricks/models/{{ MODEL_DIR }}
"""

command_60 = """
    cd {{ DW_HOME }}/dw/tubibricks/models/{{ MODEL_DIR }}
"""

# command_70 = """
#     <<< process_task: 800000 >>>
# """

# following command needed for table creation
# command_80 = """
#     <<< working_dir: /Users/jian.huang/projects_poc/dw/tubibricks >>>
#     cd {{ DW_HOME }}/dw/tubibricks && {{ DBT_BIN }} run --vars '{start_date: "2024-06-19", end_date: "2024-06-15"}' --select {{ MODEL_NAME }} --full-refresh --debug --target {{ DEPLOYMENT_ENV }}
# """


command_90 = """
    <<< working_dir: /Users/jian.huang/projects_poc/dw >>>
    cd {{ DW_HOME }}/dw/ && /Users/jian.huang/.local/bin/pipenv run workflow --project tubibricks --target {{ DEPLOYMENT_ENV }} --job_name {{ MODEL_NAME }} --command run --selectors '{{ MODEL_NAME }}' --start {{ START_DATE }} --end {{ END_DATE }} {{ TIME_INTERVAL }}
"""
#
# command_100 = """
#     echo {{ DW_HOME }}/dw/tubibricks/src/{{ MODEL_NAME }}/models/{{ MODEL_DIR }}/
#
# """

command_110 = """
    <<< working_dir: /Users/jian.huang/projects_poc/dw/tubibricks >>>
    cd {{ DW_HOME }}/dw/tubibricks && /System/Volumes/Data/opt/homebrew/Cellar/databricks/0.230.0/bin/databricks bundle deploy --target {{ BUNDLE_DEPLOY_TGT }}
"""



