# # update submodule
# import git

# repo = git.Repo('.')

# output = repo.git.submodule('update', '--remote')

# Update dataset
# from reports_combiner import reports_combiner
# from datetime import date,timedelta
# latest_date = date.today()-timedelta(days=1)
# latest_date = latest_date.strftime('%m-%d-%Y')
# reports_combiner(latest_date)

# from inflection.world import store_all
# store_all()

# from inflection.us import store_all,testing
# store_all()
# # testing()

# from projection.world import store_all
# store_all()

# from projection.us import store_all
# store_all()

from data_prepare_plot import global_prepare,us_prepare
global_prepare()
us_prepare()