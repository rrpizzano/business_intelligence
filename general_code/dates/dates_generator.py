import datetime

# Fecha domingo pasado
today = datetime.date.today()
start_of_week = today - datetime.timedelta(days=today.weekday())
csv_suffix = start_of_week - datetime.timedelta(days=1)

monday = datetime.date.today() - datetime.timedelta(days=7)
tuesday = datetime.date.today() - datetime.timedelta(days=6)
wednesday = datetime.date.today() - datetime.timedelta(days=5)
thursday = datetime.date.today() - datetime.timedelta(days=4)
friday = datetime.date.today() - datetime.timedelta(days=3)
saturday = datetime.date.today() - datetime.timedelta(days=2)
sunday = datetime.date.today() - datetime.timedelta(days=1)

monday_2 = datetime.date.today() + datetime.timedelta(days=0)
tuesday_2 = datetime.date.today() + datetime.timedelta(days=1)
wednesday_2 = datetime.date.today() + datetime.timedelta(days=2)
thursday_2 = datetime.date.today() + datetime.timedelta(days=3)
friday_2 = datetime.date.today() + datetime.timedelta(days=4)
saturday_2 = datetime.date.today() + datetime.timedelta(days=5)
sunday_2 = datetime.date.today() + datetime.timedelta(days=6)

week = [monday, tuesday, wednesday, thursday, friday, saturday, sunday,
        monday_2, tuesday_2, wednesday_2, thursday_2, friday_2, saturday_2, sunday_2]

for day in week:
    today = day
    start_of_week = today - datetime.timedelta(days=today.weekday())
    csv_suffix = start_of_week - datetime.timedelta(days=1)
    print(day, ' > ', csv_suffix)


"""
from datetime import datetime
now = datetime.now()  # current date and time
date = now.strftime("%Y-%m-%d")
print("date:", date)
"""
