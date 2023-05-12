cd ..
py -m pip install --user virtualenv
py -m venv realweb-dbt-env
source ./realweb-dbt-env/Scripts/activate
pip install --default-timeout=1000 --no-cache-dir pip
pip install --default-timeout=1000 --no-cache-dir dbt-bigquery
cd realweb-dbt-project
dbt clean
dbt deps
dbt debug