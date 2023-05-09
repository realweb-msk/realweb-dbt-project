cd ..
py -m pip install --user virtualenv
py -m venv realweb-dbt-env
source ./realweb-dbt-env/Scripts/activate
pip install --default-timeout=1000 --no-cache-dir pip==23.1.2
pip install --default-timeout=1000 --no-cache-dir dbt-bigquery==1.5
cd realweb-dbt-project
dbt clean
dbt deps
dbt debug