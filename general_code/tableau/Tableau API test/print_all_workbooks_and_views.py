import tableauserverclient as TSC

tableau_server_url = "https://tableau....net"
tableau_user = "...Tableau"
tableau_password = "6..."
tableau_site_id = "P..."

# connection to tableau server (if local use VPN)
try:
    tableau_auth = TSC.TableauAuth(
        tableau_user, tableau_password, tableau_site_id)
    server = TSC.Server(tableau_server_url, use_server_version=True)
    server.auth.sign_in(tableau_auth)
except Exception as err:
    raise Exception(
        f'Error while connecting to Tableau server, exception is: {str(err)}')

# see all workbooks by name and id
with server.auth.sign_in(tableau_auth):
    req_options = TSC.RequestOptions().page_size(1000)
    all_workbooks_items, pagination_item = server.workbooks.get(
        req_options)  # por default get trae los primeros 100
    workbooks = [workbook for workbook in all_workbooks_items]
    print("________workbooks________")
    for workbook in workbooks:
        print(workbook.name, '->', workbook.id)

"""
# see all views and ids of an specific worbook
print('\n')
with server.auth.sign_in(tableau_auth):
    worbook = server.workbooks.get_by_id('afd6...')
    for view in worbook.views:
        print(view.name, '->', view.id)
"""

"""
# save view as PDF
with server.auth.sign_in(tableau_auth):
    view_item = server.views.get_by_id('2aa...')
    server.views.populate_pdf(view_item, req_options=None)
    try:
        with open('./Tableau API test/{}.pdf'.format(view_item.name), 'wb') as f:
            f.write(view_item.pdf)
    except Exception as err:
        raise Exception(
            f'Error while creating PDF, exception is: {str(err)}')
"""
