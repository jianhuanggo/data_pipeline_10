command_110 = """
    <<< working_dir: /Users/jian.huang/projects_poc/dw/tubibricks >>>
    cd {{ DW_HOME }}/dw/tubibricks && /System/Volumes/Data/opt/homebrew/Cellar/databricks/0.230.0/bin/databricks bundle deploy --target1 {{ BUNDLE_DEPLOY_TGT }}
"""

command_120 = """

    echo "the deployment process is successful"
"""