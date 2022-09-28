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

# save view as PDF
with server.auth.sign_in(tableau_auth):

    workbook_name = 'Workbook name'
    workbook_id = None
    view_name = "View name"
    view_id = None

    all_workbooks, pagination_item = server.workbooks.get()
    for workbook in [workbook for workbook in all_workbooks]:
        if workbook.name == workbook_name:
            workbook_id = workbook.id

        if workbook_id != None:
            for view in server.workbooks.get_by_id(workbook_id).views:
                if view.name == view_name:
                    view_id = view.id

    if view_id != None:
        view_item = server.views.get_by_id(view_id)
        server.views.populate_pdf(view_item, req_options=None)
        try:
            with open('./Tableau API test/{}.pdf'.format(view_item.name), 'wb') as f:
                f.write(view_item.pdf)
                print('OK --> PDF successfully saved: {}'.format(view_item.name))
        except Exception as err:
            raise Exception(
                'Error while creating PDF, exception is: {}'.format(str(err)))

    if workbook_id == None or view_id == None:
        print('FAIL --> Workbook or View name were not found')


server.auth.sign_out()
