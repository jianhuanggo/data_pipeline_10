command_80 = """
    <<< working_dir: /Users/jian.huang/projects_poc/dw/tubibricks >>>
    cd {{ DW_HOME }}/dw/tubibricks && {{ DBT_BIN }} run --vars '{start_date: "2024-06-19", end_date: "2024-06-15"}' --select {{ MODEL_NAME }} --full-refresh --debug --target {{ DEPLOYMENT_ENV }}
"""