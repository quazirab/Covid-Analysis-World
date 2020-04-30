# # update submodule
import git

repo = git.Repo('.')

output = repo.git.submodule('update', '--remote')

# Update dataset
from reports_combiner import reports_combiner
from datetime import date,timedelta
latest_date = date.today()-timedelta(days=1)
latest_date = latest_date.strftime('%m-%d-%Y')
reports_combiner(latest_date)

from inflection_finder import store_all_country
store_all_country()
