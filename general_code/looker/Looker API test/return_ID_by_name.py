import looker_sdk

# For this to work you must either have set environment variables or created a looker.ini as described below in "Configuring the SDK"
# or init31() for the older v3.1 API
sdk = looker_sdk.init31('./old_code/Looker/Looker API test/looker.ini')
my_user = sdk.me()
print('LOGIN OK --> ', my_user.email)

title = 'Nacion Sushi'
dashboard = next(iter(sdk.search_dashboards(title=title.lower())), None)
print(title, ' --> id =', dashboard.id)

#id = '11796'
#dashboard = next(iter(sdk.search_dashboards(id=11796), None))
# print(dashboard)
