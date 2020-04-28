from reports_combiner import reports_combiner
from datetime import date,timedelta
latest_date = date.today()-timedelta(days=1)
latest_date = latest_date.strftime('%m-%d-%Y')
reports_combiner(latest_date)
