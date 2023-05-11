cd ..
python3 -m pip install --user virtualenv
python3 -m venv realweb-dbt-env
source /Users/Elena/realweb-dbt-env/bin/activate
pip install --default-timeout=1000 --no-cache-dir pip==23.1.2
pip install --default-timeout=1000 --no-cache-dir dbt-bigquery==1.3.1
cd realweb-dbt-project
dbt clean
dbt deps
dbt debug